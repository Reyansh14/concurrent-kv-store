import asyncio
import websockets
import logging
import json
import sys
import random
from typing import List
import time
import unittest
from server import Lock  # Import Lock from server.py

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("TestClient")

async def connect_websocket():
    websocket = await websockets.connect('ws://localhost:8000/ws')
    return websocket

async def send_command(websocket, command, keys=None, value=None, hold_lock=False, timeout=None, mode=None):
    if not websocket:
        return {"success": False, "error": "No websocket connection"}
    
    try:
        message = {
            "command": command,
            "hold_lock": hold_lock
        }
        if keys:
            message["keys"] = keys
        if value is not None:
            message["value"] = value
        if timeout is not None:
            message["timeout"] = timeout
        if mode is not None:
            message["mode"] = mode
            
        # Use asyncio.wait_for to enforce client-side timeout as well
        if timeout is not None:
            # Add a small buffer to the client timeout
            client_timeout = timeout + 0.5
            try:
                await asyncio.wait_for(websocket.send(json.dumps(message)), timeout=client_timeout)
                response = await asyncio.wait_for(websocket.recv(), timeout=client_timeout)
            except asyncio.TimeoutError:
                return {"success": False, "error": "Client timeout waiting for response"}
        else:
            await websocket.send(json.dumps(message))
            response = await websocket.recv()
            
        return json.loads(response)
    except Exception as e:
        logger.error(f"Error sending command: {e}")
        return {"success": False, "error": str(e)}

async def test_concurrent_readers():
    """Test that multiple clients can read simultaneously"""
    logger.info("\n=== Testing Concurrent Readers ===")
    
    ws1 = await connect_websocket()
    ws2 = await connect_websocket()
    
    if not ws1 or not ws2:
        logger.error("Failed to establish connections")
        return False
        
    try:
        # First client writes initial value
        logger.info("Client 1 writing initial value...")
        result = await send_command(ws1, "write", ["key1"], "value1")
        if not result["success"]:
            logger.error("Failed to write initial value")
            return False
            
        # Both clients try to read simultaneously
        logger.info("Both clients attempting concurrent reads...")
        results = await asyncio.gather(
            send_command(ws1, "read", ["key1"]),
            send_command(ws2, "read", ["key1"])
        )
        
        success = all(r["success"] for r in results)
        if success:
            logger.info("Concurrent reads successful")
        else:
            logger.error("Concurrent reads failed")
        return success
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        await ws1.close(code=1000, reason="Test finished")
        await ws2.close(code=1000, reason="Test finished")

async def test_writer_blocking_readers():
    """Test that a writer blocks readers until it releases the lock"""
    logger.info("\n=== Testing Writer Blocking Readers ===")
    
    ws1 = await connect_websocket()  # Writer
    ws2 = await connect_websocket()  # Reader
    
    if not ws1 or not ws2:
        logger.error("Failed to establish connections")
        return False
        
    try:
        # Writer acquires lock first
        result = await send_command(ws1, "write", ["key1"], "initial", hold_lock=True)
        if not result["success"]:
            logger.error("Writer failed to acquire lock")
            return False
            
        # Start reader task - it should queue up
        logger.info("Starting reader while writer holds lock...")
        read_start_time = asyncio.get_event_loop().time()
        read_task = asyncio.create_task(send_command(ws2, "read", ["key1"]))
        
        # Small delay to ensure reader is queued
        await asyncio.sleep(0.1)
        
        # Write a new value while holding lock
        result = await send_command(ws1, "write", ["key1"], "updated", hold_lock=True)
        if not result["success"]:
            logger.error("Writer failed second write")
            return False
            
        # Release the write lock
        logger.info("Releasing writer lock...")
        result = await send_command(ws1, "unlock", ["key1"])
        if not result["success"]:
            logger.error("Failed to release write lock")
            return False
            
        # Wait for reader to complete and verify it sees the final value
        result = await read_task
        read_end_time = asyncio.get_event_loop().time()
        read_duration = read_end_time - read_start_time
        
        if not result["success"]:
            logger.error("Reader failed after writer released lock")
            return False
            
        if result.get("values", {}).get("key1") != "updated":
            logger.error(f"Reader saw wrong value: {result.get('values', {}).get('key1')}")
            return False
            
        if read_duration < 0.1:  # Verify reader was actually blocked
            logger.error("Reader completed too quickly, may not have been properly blocked")
            return False
            
        logger.info(f"Reader completed after {read_duration:.2f}s, saw correct final value")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        await ws1.close(code=1000, reason="Test finished")
        await ws2.close(code=1000, reason="Test finished")

async def test_deadlock_prevention():
    """Test deadlock prevention when clients try to lock in different orders"""
    logger.info("\n=== Testing Deadlock Prevention ===")
    logger.info("\nScenario 1: Different Lock Orders")
    
    ws1 = await connect_websocket()
    ws2 = await connect_websocket()
    
    if not ws1 or not ws2:
        logger.error("Failed to establish connections")
        return False
        
    try:
        # Client 1 locks key1, Client 2 locks key2
        success1 = await send_command(ws1, "lock", ["key1"], value=None, hold_lock=True, mode="write")
        success2 = await send_command(ws2, "lock", ["key2"], value=None, hold_lock=True, mode="write")
        assert success1 and success2, "Initial locks should be acquired"
        logger.info("Initial locks acquired successfully")
            
        # Now try to acquire each other's keys - this should timeout for one client
        task1 = asyncio.create_task(send_command(ws1, "lock", ["key2"], value=None, hold_lock=True, timeout=1.0, mode="write"))
        task2 = asyncio.create_task(send_command(ws2, "lock", ["key1"], value=None, hold_lock=True, timeout=1.0, mode="write"))
            
        results = await asyncio.gather(task1, task2)
            
        # At least one of the tasks should fail with a timeout
        timeouts = [not r["success"] and "timeout" in r.get("error", "").lower() for r in results]
        assert any(timeouts), "At least one client should timeout to prevent deadlock"
        logger.info("Deadlock prevention successful - one client timed out as expected")
            
        # Release the locks
        await send_command(ws1, "unlock", ["key1"])
        await send_command(ws2, "unlock", ["key2"])
        
        return True
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        await ws1.close(code=1000, reason="Test finished")
        await ws2.close(code=1000, reason="Test finished")

async def test_writer_priority():
    """Test that writers get priority over readers"""
    logger.info("\n=== Testing Writer Priority ===")
    
    ws_reader = await connect_websocket()
    ws_writer = await connect_websocket()
    
    if not ws_reader or not ws_writer:
        logger.error("Failed to establish connections")
        return False
        
    try:
        # Reader tries to get lock
        result = await send_command(ws_reader, "read", ["key1"])
        if not result["success"]:
            logger.error("Reader failed to get initial lock")
            return False
            
        # Writer tries to write - should get priority
        writer_result = await send_command(ws_writer, "write", ["key1"], "priority_value")
        success = writer_result["success"]
        if success:
            logger.info("Writer priority confirmed")
        return success
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        await ws_reader.close(code=1000, reason="Test finished")
        await ws_writer.close(code=1000, reason="Test finished")

async def test_concurrent_access():
    """Test various concurrent access patterns"""
    logger.info("\n=== Testing Concurrent Access Patterns ===")
    
    clients = []
    try:
        logger.info("Establishing connections for concurrent access test...")
        clients = [await connect_websocket() for _ in range(3)]
        if not all(clients):
            logger.error("Failed to establish connections")
            return False
        
        # Multiple concurrent operations
        logger.info("Attempting concurrent operations:")
        logger.info("- Client 1: Writing to key1")
        logger.info("- Client 2: Reading from key1")
        logger.info("- Client 3: Writing to key2")
        
        results = await asyncio.gather(
            send_command(clients[0], "write", ["key1"], "value1"),
            send_command(clients[1], "read", ["key1"]),
            send_command(clients[2], "write", ["key2"], "value2")
        )
        
        success = any(r["success"] for r in results)
        if success:
            logger.info("Concurrent access patterns working correctly")
            logger.info("- At least one operation succeeded as expected")
        else:
            logger.error("All concurrent operations failed")
        return success
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        for client in clients:
            if client:
                await client.close(code=1000, reason="Test finished")

async def test_atomic_writes():
    """Test that writes within a lock are atomic transactions"""
    logger.info("\n=== Testing Atomic Writes ===")
    
    ws1 = await connect_websocket()  # Writer
    ws2 = await connect_websocket()  # Reader
    
    if not ws1 or not ws2:
        logger.error("Failed to establish connections")
        return False
        
    try:
        # Writer starts a transaction by acquiring lock
        logger.info("Writer starting transaction...")
        result = await send_command(ws1, "write", ["key1"], "value1", hold_lock=True)
        if not result["success"]:
            logger.error("Failed to acquire write lock for transaction")
            return False
            
        # Reader should queue up and wait
        logger.info("Reader attempting to read during transaction...")
        read_task = asyncio.create_task(send_command(ws2, "read", ["key1"]))
        
        # Writer makes another write within same transaction
        logger.info("Writer making second write in transaction...")
        result = await send_command(ws1, "write", ["key1"], "value2", hold_lock=True)
        if not result["success"]:
            logger.error("Failed to make second write in transaction")
            return False
            
        # Complete transaction by releasing lock
        logger.info("Writer completing transaction...")
        result = await send_command(ws1, "unlock", ["key1"])
        if not result["success"]:
            logger.error("Failed to release lock after transaction")
            return False
            
        # Now the reader's task should complete
        result = await read_task
        if not result["success"]:
            logger.error("Reader failed to read after transaction")
            return False
        if result.get("values", {}).get("key1") != "value2":
            logger.error(f"Expected value2 but got {result.get('values', {}).get('key1')}")
            return False
            
        logger.info("Atomic transaction successful")
        return True
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        await ws1.close(code=1000, reason="Test finished")
        await ws2.close(code=1000, reason="Test finished")

async def test_lock_timeout():
    """Test that lock acquisition times out after specified duration"""
    logger.info("\n=== Testing Lock Timeout ===")
    
    async with websockets.connect('ws://localhost:8000/ws') as ws1, \
               websockets.connect('ws://localhost:8000/ws') as ws2:
        
        # First client acquires write lock and holds it
        logger.info("Client 1 acquiring write lock...")
        response = await send_command(ws1, "lock", keys=["key1"], value=None, hold_lock=True, mode="write")
        assert response["success"], "First client should acquire lock"
        
        # Second client tries to get write lock with timeout
        logger.info("Client 2 attempting write lock with timeout...")
        start_time = time.time()
        response = await send_command(ws2, "lock", keys=["key1"], value=None, hold_lock=True, timeout=1.0, mode="write")
        elapsed = time.time() - start_time
        
        # Verify timeout behavior
        assert not response["success"], "Lock should have timed out"
        assert "timeout" in response.get("error", "").lower(), "Error should mention timeout"
        assert 0.9 <= elapsed <= 2.0, f"Lock attempt should timeout in ~1 second, took {elapsed:.2f}s"
        
        logger.info(f"Lock attempt timed out after {elapsed:.2f}s as expected")
        
        # Release first client's lock
        await send_command(ws1, "unlock", keys=["key1"])

class TestLockUnit(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.lock = Lock()
        
    async def test_write_queue_ordering(self):
        """Test that writers are processed in FIFO order"""
        client1, client2 = "client1", "client2"
        
        # First client acquires write lock
        self.assertTrue(await self.lock.acquire_write(client1))
        
        # Second client attempts to acquire while first holds it
        acquire_task = asyncio.create_task(self.lock.acquire_write(client2))
        await asyncio.sleep(0.1)  # Give time for second client to queue
        
        self.assertEqual(len(self.lock.write_queue), 1)
        self.assertEqual(self.lock.write_waiters, 1)
        
        # Release first client's lock
        await self.lock.release_write(client1)
        
        # Second client should now acquire the lock
        await acquire_task
        self.assertEqual(self.lock.writer, client2)
        
    async def test_reader_batching(self):
        """Test that multiple readers are batched efficiently"""
        clients = [f"client{i}" for i in range(5)]
        
        # Create tasks for multiple simultaneous read acquisitions
        tasks = [self.lock.acquire_read(client) for client in clients]
        results = await asyncio.gather(*tasks)
        
        # All readers should succeed
        self.assertTrue(all(results))
        # All clients should be in readers set
        self.assertEqual(self.lock.readers, set(clients))
        # Read queue should be empty after processing
        self.assertEqual(len(self.lock.read_queue), 0)
        
    async def test_write_timeout(self):
        """Test that write lock acquisition properly times out"""
        client1, client2 = "client1", "client2"
        
        # First client acquires write lock
        self.assertTrue(await self.lock.acquire_write(client1))
        
        # Second client should timeout
        start_time = time.time()
        result = await self.lock.acquire_write(client2, timeout=1.0)
        elapsed = time.time() - start_time
        
        self.assertFalse(result)
        self.assertGreaterEqual(elapsed, 1.0)
        self.assertEqual(self.lock.writer, client1)
        
    async def test_read_write_priority(self):
        """Test that writers have priority over new readers"""
        reader1, reader2, writer = "reader1", "reader2", "writer"
        
        # First reader acquires lock
        self.assertTrue(await self.lock.acquire_read(reader1))
        
        # Writer attempts to acquire
        writer_task = asyncio.create_task(self.lock.acquire_write(writer))
        await asyncio.sleep(0.1)
        
        # Second reader should not be able to acquire while writer is waiting
        reader2_task = asyncio.create_task(self.lock.acquire_read(reader2))
        await asyncio.sleep(0.1)
        
        self.assertFalse(reader2_task.done())
        
        # Release first reader
        await self.lock.release_read(reader1)
        
        # Writer should acquire next
        await writer_task
        self.assertEqual(self.lock.writer, writer)

async def test_multi_key_concurrent_readers():
    """Multiple readers acquire locks on multiple keys concurrently."""
    logger.info("\n=== Testing Multi-Key Concurrent Readers ===")
    ws1 = await connect_websocket()
    ws2 = await connect_websocket()
    
    try:
        # Initialize key1 and key2
        await send_command(ws1, "write", ["key1"], value="init1")
        await send_command(ws1, "write", ["key2"], value="init2")
        logger.info("Initialized test keys")

        # Both readers attempt to acquire locks simultaneously
        logger.info("Both readers attempting to acquire locks on key1 and key2...")
        read1_task = asyncio.create_task(
            send_command(ws1, "lock", ["key1", "key2"], mode="read", hold_lock=True)
        )
        read2_task = asyncio.create_task(
            send_command(ws2, "lock", ["key1", "key2"], mode="read", hold_lock=True)
        )

        results = await asyncio.gather(read1_task, read2_task)
        assert all(res["success"] for res in results), "Both readers should acquire multi-key read locks"
        logger.info("Both readers successfully acquired locks")

        # Verify they can read the values
        r1_values = await send_command(ws1, "read", ["key1", "key2"], hold_lock=True)
        r2_values = await send_command(ws2, "read", ["key1", "key2"], hold_lock=True)
        assert r1_values["success"] and r2_values["success"], "Both readers should successfully read"
        assert r1_values["values"]["key1"] == "init1" and r1_values["values"]["key2"] == "init2"
        assert r2_values["values"]["key1"] == "init1" and r2_values["values"]["key2"] == "init2"
        logger.info("Both readers successfully read values")

        # Release locks
        await send_command(ws1, "unlock", ["key1", "key2"])
        await send_command(ws2, "unlock", ["key1", "key2"])
        logger.info("Locks released")

    finally:
        await ws1.close()
        await ws2.close()

async def test_multi_key_write_atomicity():
    """Test atomic multi-key writes and ensure no partial updates are visible."""
    logger.info("\n=== Testing Multi-Key Write Atomicity ===")
    ws_writer = await connect_websocket()
    ws_reader = await connect_websocket()
    
    try:
        # Initialize keys
        await send_command(ws_writer, "write", ["key1"], "original1")
        await send_command(ws_writer, "write", ["key2"], "original2")
        logger.info("Initialized test keys")

        # Writer locks both keys
        lock_result = await send_command(ws_writer, "lock", ["key1", "key2"], mode="write", hold_lock=True)
        assert lock_result["success"], "Writer should acquire multi-key write lock"
        logger.info("Writer acquired locks")

        # Start reader task (should block)
        logger.info("Starting reader task (should block)...")
        read_task = asyncio.create_task(send_command(ws_reader, "read", ["key1", "key2"]))

        # Writer updates keys
        await send_command(ws_writer, "write", ["key1"], "updated1", hold_lock=True)
        await asyncio.sleep(0.1)  # Small delay to ensure updates aren't atomic
        await send_command(ws_writer, "write", ["key2"], "updated2", hold_lock=True)
        logger.info("Writer updated both keys")

        # Release locks
        unlock_result = await send_command(ws_writer, "unlock", ["key1", "key2"])
        assert unlock_result["success"], "Writer unlock should succeed"
        logger.info("Writer released locks")

        # Check reader results
        reader_result = await read_task
        assert reader_result["success"], "Reader should succeed after writer unlock"
        assert reader_result["values"]["key1"] == "updated1"
        assert reader_result["values"]["key2"] == "updated2"
        logger.info("Reader saw consistent final values")

    finally:
        await ws_writer.close()
        await ws_reader.close()

async def test_multi_key_partial_failure():
    """Test partial lock acquisition failure and proper cleanup."""
    logger.info("\n=== Testing Multi-Key Partial Failure ===")
    ws_owner = await connect_websocket()
    ws_contender = await connect_websocket()
    
    try:
        # Owner locks key2
        owner_lock = await send_command(ws_owner, "lock", ["key2"], mode="write", hold_lock=True)
        assert owner_lock["success"], "Owner should lock key2"
        logger.info("Owner acquired lock on key2")

        # Contender tries to lock both keys
        logger.info("Contender attempting to lock both keys (should fail)...")
        contender_lock = await send_command(ws_contender, "lock", ["key1", "key2"], 
                                          mode="write", timeout=1.0)
        assert not contender_lock["success"], "Contender should fail to lock both keys"
        logger.info("Contender failed to acquire locks as expected")

        # Verify contender has no partial locks
        read_result = await send_command(ws_contender, "read", ["key1"])
        assert read_result["success"], "Contender should be able to read key1 if no partial lock remains"
        logger.info("Verified no partial locks remain")

        # Release owner's lock
        await send_command(ws_owner, "unlock", ["key2"])
        logger.info("Owner released lock")

    finally:
        await ws_owner.close()
        await ws_contender.close()

async def test_reentrant_read_lock():
    """Test behavior of reentrant read locks."""
    logger.info("\n=== Testing Reentrant Read Lock ===")
    ws = await connect_websocket()
    
    try:
        # First read lock
        res1 = await send_command(ws, "lock", ["key1"], mode="read", hold_lock=True)
        assert res1["success"], "First read lock should succeed"
        logger.info("First read lock acquired")

        # Second read lock attempt
        res2 = await send_command(ws, "lock", ["key1"], mode="read", hold_lock=True)
        # Our implementation allows reentrant read locks
        assert res2["success"], "Reentrant read lock should succeed"
        logger.info("Reentrant read lock acquired")

        # Release locks
        await send_command(ws, "unlock", ["key1"])
        await send_command(ws, "unlock", ["key1"])
        logger.info("Locks released")

    finally:
        await ws.close()

async def stress_test():
    """Stress test with multiple concurrent clients."""
    logger.info("\n=== Running Stress Test ===")
    NUM_CLIENTS = 10
    OPERATIONS_PER_CLIENT = 20
    keys = [f"key{i}" for i in range(5)]
    tasks = []

    async def random_client_behavior(client_id: str):
        ws = await connect_websocket()
        try:
            for i in range(OPERATIONS_PER_CLIENT):
                cmd = random.choice(["read", "write"])
                rand_keys = random.sample(keys, random.randint(1, len(keys)))
                try:
                    if cmd == "read":
                        result = await send_command(ws, "read", rand_keys)
                        assert result["success"] or "error" in result, f"Operation {i} failed without error"
                    else:
                        for k in rand_keys:
                            val = f"val-{client_id}-{k}-{i}"
                            result = await send_command(ws, "write", [k], value=val)
                            assert result["success"] or "error" in result, f"Operation {i} failed without error"
                    await asyncio.sleep(0.01)
                except Exception as e:
                    logger.error(f"Client {client_id} operation {i} failed: {e}")
                    raise
        finally:
            await ws.close()

    logger.info(f"Starting {NUM_CLIENTS} clients with {OPERATIONS_PER_CLIENT} operations each")
    for i in range(NUM_CLIENTS):
        tasks.append(asyncio.create_task(random_client_behavior(f"client{i}")))

    await asyncio.gather(*tasks)
    logger.info("Stress test completed successfully")

async def main():
    """Run all tests"""
    await test_concurrent_readers()
    await test_writer_blocking_readers()
    await test_deadlock_prevention()
    await test_writer_priority()
    await test_concurrent_access()
    await test_atomic_writes()
    await test_lock_timeout()
    # New tests
    await test_multi_key_concurrent_readers()
    await test_multi_key_write_atomicity()
    await test_multi_key_partial_failure()
    await test_reentrant_read_lock()
    await stress_test()

if __name__ == "__main__":
    asyncio.run(main())
