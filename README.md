# Concurrent Key-Value Store

A high-performance, concurrent key-value store server implemented in Python using FastAPI and WebSockets. The server supports multiple readers and exclusive writers with deadlock prevention.

## Features

- Multiple concurrent readers support
- Exclusive writer access
- Deadlock prevention through sorted key ordering
- No writer starvation
- Atomic multi-key operations
- WebSocket-based communication
- Comprehensive test suite

## Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd <repo-name>
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Start the server:
```bash
python server.py
```

2. Run the tests:
```bash
python test_client.py
```

## API

The server accepts WebSocket connections at `/ws` and supports the following commands:

### Read
```json
{
    "command": "read",
    "keys": ["key1", "key2"],
    "hold_lock": false,
    "timeout": 5.0
}
```

### Write
```json
{
    "command": "write",
    "keys": ["key1"],
    "value": "new_value",
    "hold_lock": false,
    "timeout": 5.0
}
```

### Lock
```json
{
    "command": "lock",
    "keys": ["key1", "key2"],
    "mode": "write",  // or "read"
    "timeout": 5.0
}
```

### Unlock
```json
{
    "command": "unlock",
    "keys": ["key1", "key2"]
}
```

## Implementation Details

- Reader-writer lock with writer priority
- Event-driven notifications (no polling)
- Batched reader lock acquisition
- Comprehensive error handling and logging
- Automatic lock cleanup on client disconnect

## License

MIT License - see LICENSE file for details
