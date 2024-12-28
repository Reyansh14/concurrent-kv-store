from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
from typing import Dict, Set, Optional
import json
import asyncio
from dataclasses import dataclass, field
from collections import defaultdict
import logging
import sys
import time
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("KeyValueStore")

app = FastAPI()

class Lock:
    def __init__(self):
        self.condition = asyncio.Condition()
        self.writer = None # curr writer'sclient_id
        self.readers = set() # curr readers' client_ids
        self.write_waiters = 0
        self.read_queue = {}
        self.write_queue = {}

    async def acquire_write(self, client_id: str, timeout: float = None) -> bool:
        start_time = time.time()
        async with self.condition:
            self.write_waiters += 1
            future = asyncio.Future()
            self.write_queue[client_id] = future
            
            try:
                while True:
                    if timeout and (time.time() - start_time) > timeout:
                        return False
                        
                    if self.writer is None and len(self.readers) == 0:
                        self.writer = client_id
                        future.set_result(True)
                        return True
                    
                    try:
                        if timeout:
                            remaining = timeout - (time.time() - start_time)
                            if remaining <= 0:
                                return False
                            await asyncio.wait_for(self.condition.wait(), timeout=remaining)
                        else:
                            await self.condition.wait()
                    except asyncio.TimeoutError:
                        return False
            finally:
                self.write_waiters -= 1
                if client_id in self.write_queue:
                    del self.write_queue[client_id]

    async def acquire_read(self, client_id: str, timeout: float = None) -> bool:
        start_time = time.time()
        async with self.condition:
            future = asyncio.Future()
            self.read_queue[client_id] = future
            
            try:
                while True:
                    if timeout and (time.time() - start_time) > timeout:
                        return False
                        
                    if self.writer is None and self.write_waiters == 0:
                        # Batch process all waiting readers
                        readers_to_add = set()
                        for waiting_id, waiting_future in list(self.read_queue.items()):
                            if not waiting_future.done():
                                readers_to_add.add(waiting_id)
                                waiting_future.set_result(True)
                        
                        self.readers.update(readers_to_add)
                        return True
                    
                    try:
                        if timeout:
                            remaining = timeout - (time.time() - start_time)
                            if remaining <= 0:
                                return False
                            await asyncio.wait_for(self.condition.wait(), timeout=remaining)
                        else:
                            await self.condition.wait()
                    except asyncio.TimeoutError:
                        return False
            finally:
                if client_id in self.read_queue:
                    del self.read_queue[client_id]

    async def release_write(self, client_id: str):
        async with self.condition:
            if self.writer == client_id:
                self.writer = None
                # First notify one waiting writer if any
                if self.write_waiters > 0:
                    self.condition.notify(1)  # Wake up just one writer
                else:
                    # If no writers waiting, wake up all readers
                    self.condition.notify_all()

    async def release_read(self, client_id: str):
        async with self.condition:
            if client_id in self.readers:
                self.readers.remove(client_id)
                if not self.readers:  # If this was the last reader
                    if self.write_waiters > 0:
                        self.condition.notify(1)  # Wake up just one writer
                    else:
                        self.condition.notify_all()  # Wake up all readers

class KeyValueStore:
    def __init__(self):
        self.store: Dict[str, str] = {}
        self.locks: Dict[str, Lock] = defaultdict(Lock)
        self.client_locks: Dict[str, Set[str]] = defaultdict(set)

    async def rlock(self, client_id: str, key: str, timeout: Optional[float] = None) -> bool:
        lock = self.locks[key]
        return await lock.acquire_read(client_id, timeout)

    async def wlock(self, client_id: str, keys: list[str], timeout: Optional[float] = None) -> bool:
        sorted_keys = sorted(keys)
        locks = [self.locks[key] for key in sorted_keys]
        
        logger.info(f"Client {client_id} attempting to lock keys: {sorted_keys}")
        logger.info(f"Current lock state:")
        for key in sorted_keys:
            lock = self.locks[key]
            logger.info(f"  {key}: writer={lock.writer}, readers={lock.readers}")
        
        for key in sorted_keys:
            if key in self.client_locks[client_id]:
                logger.info(f"Client {client_id} already has lock on {key}")
                return False

        try:
            for key, lock in zip(sorted_keys, locks):
                if not await lock.acquire_write(client_id, timeout):
                    for k, l in zip(sorted_keys, locks):
                        if l.writer == client_id:
                            await l.release_write(client_id)
                            if k in self.client_locks[client_id]:
                                self.client_locks[client_id].remove(k)
                    return False
                self.client_locks[client_id].add(key)
                logger.info(f"Client {client_id} acquired write lock on {key}")

            return True
        except Exception as e:
            logger.error(f"Error acquiring locks: {e}")
            for key, lock in zip(sorted_keys, locks):
                if lock.writer == client_id:
                    await lock.release_write(client_id)
                    if key in self.client_locks[client_id]:
                        self.client_locks[client_id].remove(key)
            return False

    async def unlock(self, client_id: str, keys: list[str]) -> bool:
        try:
            logger.info(f"Client {client_id} releasing locks for keys: {keys}")
            for key in keys:
                lock = self.locks[key]
                if client_id in lock.readers:
                    logger.info(f"Client {client_id} released read lock on {key}")
                    await lock.release_read(client_id)
                    self.client_locks[client_id].remove(key)
                elif lock.writer == client_id:
                    logger.info(f"Client {client_id} released write lock on {key}")
                    await lock.release_write(client_id)
                    self.client_locks[client_id].remove(key)
                else:
                    logger.warning(f"Client {client_id} attempted to unlock {key} but doesn't hold the lock")
                    return False

            if not self.client_locks[client_id]:
                logger.info(f"Removing empty client_locks entry for {client_id}")
                del self.client_locks[client_id]
                
            return True
        except Exception as e:
            logger.error(f"Error unlocking: {e}")
            return False

    def get(self, key: str) -> Optional[str]:
        return self.store.get(key)

    def set(self, key: str, value: str) -> None:
        self.store[key] = value

    async def acquire_read_lock(self, client_id: str, keys: list[str], timeout: Optional[float] = None) -> bool:
        sorted_keys = sorted(keys)
        locks = [self.locks[key] for key in sorted_keys]
        
        try:
            for key, lock in zip(sorted_keys, locks):
                if not await lock.acquire_read(client_id, timeout):
                    for k, l in zip(sorted_keys, locks):
                        if client_id in l.readers:
                            await l.release_read(client_id)
                    return False
                self.client_locks[client_id].add(key)
                logger.info(f"Client {client_id} acquired read lock on {key}")
            return True
        except Exception as e:
            logger.error(f"Error acquiring read locks: {e}")
            for key, lock in zip(sorted_keys, locks):
                if client_id in lock.readers:
                    await lock.release_read(client_id)
                    self.client_locks[client_id].remove(key)
            return False

    async def acquire_write_lock(self, client_id: str, keys: list[str], timeout: Optional[float] = None) -> bool:
        return await self.wlock(client_id, keys, timeout)

    async def write_value(self, key: str, value: str, client_id: str, hold_lock: bool = False, timeout: Optional[float] = None) -> dict:
        try:
            if not self.locks[key].writer == client_id:
                if not await self.acquire_write_lock(client_id, [key], timeout):
                    return {"success": False, "error": "Failed to acquire write lock"}
            
            self.store[key] = value
            
            if not hold_lock:
                await self.unlock(client_id, [key])
                
            return {"success": True}
            
        except Exception as e:
            logger.error(f"Error in write_value: {e}")
            return {"success": False, "error": str(e)}

    async def read_values(self, client_id: str, keys: list[str], hold_lock: bool = False, timeout: Optional[float] = None) -> dict:
        try:
            if not all(client_id in self.locks[key].readers for key in keys):
                if not await self.acquire_read_lock(client_id, keys, timeout):
                    return {"success": False, "error": "Failed to acquire read locks"}
            
            values = {key: self.store.get(key) for key in keys}
            
            if not hold_lock:
                await self.unlock(client_id, keys)
                
            return {"success": True, "values": values}
            
        except Exception as e:
            logger.error(f"Error in read_values: {e}")
            return {"success": False, "error": str(e)}

    def has_write_lock(self, key: str, client_id: str) -> bool:
        return key in self.locks and self.locks[key].writer == client_id

    def has_read_lock(self, key: str, client_id: str) -> bool:
        return key in self.locks and client_id in self.locks[key].readers

kv_store = KeyValueStore()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    client_id = str(uuid.uuid4())
    await websocket.accept()
    logger.info("connection open")
    
    try:
        while True:
            try:
                data = await websocket.receive_json()
                command = data.get("command")
                keys = data.get("keys", [])
                value = data.get("value")
                hold_lock = data.get("hold_lock", False)
                timeout = data.get("timeout")
                
                if not keys:
                    await websocket.send_json({"success": False, "error": "No keys specified"})
                    continue
                
                if command == "read":
                    try:
                        if not all(kv_store.has_read_lock(key, client_id) for key in keys):
                            if not await kv_store.acquire_read_lock(client_id, keys, timeout):
                                await websocket.send_json({
                                    "success": False, 
                                    "error": "Timeout waiting for read lock" if timeout else "Failed to acquire read locks"
                                })
                                continue
                        
                        result = await kv_store.read_values(client_id, keys, hold_lock)
                        await websocket.send_json(result)
                        
                    except Exception as e:
                        await websocket.send_json({"success": False, "error": str(e)})
                        
                elif command == "write":
                    if not value:
                        await websocket.send_json({"success": False, "error": "No value specified"})
                        continue
                    
                    if len(keys) != 1:
                        await websocket.send_json({
                            "success": False, 
                            "error": "Write command only supports one key at a time"
                        })
                        continue
                    
                    try:
                        result = await kv_store.write_value(keys[0], value, client_id, hold_lock, timeout)
                        await websocket.send_json(result)
                    except Exception as e:
                        await websocket.send_json({"success": False, "error": str(e)})
                        
                elif command == "lock":
                    try:
                        if data.get("mode") == "write":
                            success = await kv_store.acquire_write_lock(client_id, keys, timeout)
                        else:
                            success = await kv_store.acquire_read_lock(client_id, keys, timeout)
                            
                        if not success:
                            await websocket.send_json({
                                "success": False,
                                "error": f"Failed to acquire lock - timeout after {timeout}s"
                            })
                        else:
                            await websocket.send_json({"success": True})
                    except Exception as e:
                        await websocket.send_json({"success": False, "error": str(e)})
                        
                elif command == "unlock":
                    try:
                        await kv_store.unlock(client_id, keys)
                        await websocket.send_json({"success": True})
                    except Exception as e:
                        await websocket.send_json({"success": False, "error": str(e)})
                        
                else:
                    await websocket.send_json({
                        "success": False,
                        "error": f"Unknown command: {command}"
                    })
                    
            except json.JSONDecodeError:
                await websocket.send_json({
                    "success": False,
                    "error": "Invalid JSON message"
                })
                
    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected")
    finally:
        for key in list(kv_store.client_locks[client_id]):
            await kv_store.unlock(client_id, [key])
        logger.info("connection closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
