# GlueSync Redis Sink Connector

The GlueSync Redis Sink Connector enables real-time change data capture (CDC) from GlueSync sources (e.g., PostgreSQL, MongoDB) to Redis, an in-memory, low-latency data store. This connector streams CDC events to Redis Streams, supporting `INSERT`, `UPDATE`, and `DELETE` operations, making it ideal for use cases like real-time caching, event buffering, or microservices integration.

## Features
- **Real-Time Streaming**: Writes CDC events to Redis Streams with minimal latency.
- **Operation Support**: Handles `INSERT`, `UPDATE`, and `DELETE` operations, with tombstone events for deletions.
- **Batching**: Uses Redis pipelining for efficient batch writes, configurable via `redis_batch_size`.
- **Error Handling**: Robust error handling with retries and logging, integrated with GlueSync’s error reporting.
- **Flexible Configuration**: Supports Redis connection settings, SSL, and custom stream naming.

## Prerequisites
- **GlueSync**: Version compatible with the GlueSync Python SDK v2.0. 
- **Python**: Version 3.8 or higher.
- **Redis**: Version 5.0 or higher (for Streams support).
- **Dependencies**:
  - `gluesync-sdk` (The `gluesync-sdk` does not appear to be in the PyPI registry. It must be obtained from the repo and installed manually.) 
  - `redis-py` (Python Redis client)

## Installation
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/marcoeg/gluesync-redis-sink.git
   cd gluesync-redis-sink
   ```

2. **Install Dependencies**:
   Install dependencies:
   ```bash
    pip install -r requirements.txt   
    ```
   Ensure the GlueSync Python SDK is available as a .whl or source tarball before proceeding. 

   ```
   pip install /path/to/gluesync-sdk-2.0.0.whl
   ```

3. **Package the Connector**:
   Package the connector as a Python module:
   ```bash
   python -m build
   ```

4. **Register with GlueSync**:
   Copy the `redis_sink.py` file to your GlueSync environment and register the connector in GlueSync’s configuration (refer to GlueSync documentation for registration steps, typically via a config file or UI -- not detailed in the GlueSync repo).

>The GlueSync repo doesn’t specify how to register connectors with GlueSync. This likely requires GlueSync server configuration, which may be documented elsewhere.

## Configuration
>The GlueSync Redis sink connector assumes a JSON dictionary (e.g., in config.json) containing keys like redis_host and redis_port is loaded by GlueSync and passed to the connect method, but the exact file name, location, and validation requirements are undocumented, necessitating a hardcoded default for development.

Configure the connector in GlueSync using a JSON dictionary. Below is an example configuration:

```json
{
  "redis_host": "localhost",
  "redis_port": 6379,
  "redis_password": "your_password",
  "redis_ssl": false,
  "redis_stream_name": "gluesync_events",
  "redis_key_prefix": "gluesync:",
  "redis_batch_size": 100
}
```


### Configuration Options
| Key                   | Description                                      | Default            | Required |
|-----------------------|--------------------------------------------------|--------------------|----------|
| `redis_host`          | Redis server hostname or IP                      | `localhost`        | Yes      |
| `redis_port`          | Redis server port                                | `6379`             | Yes      |
| `redis_password`      | Redis authentication password                     | `null`             | No       |
| `redis_ssl`           | Enable SSL/TLS for Redis connection              | `false`            | No       |
| `redis_stream_name`   | Base name for Redis Streams (appended with table)| `gluesync_events`  | No       |
| `redis_key_prefix`    | Prefix for Redis keys/streams                    | `gluesync:`        | No       |
| `redis_batch_size`    | Number of events to batch for pipelined writes   | `100`              | No       |

## Usage
1. **Set Up a GlueSync Source**:
   Configure a GlueSync source (e.g., PostgreSQL, MongoDB) to capture CDC events. Refer to [GlueSync Documentation](https://docs.molo17.com/gluesync/v2.0/) for source setup.

2. **Configure the Redis Sink**:
   Add the Redis sink connector to your GlueSync pipeline, specifying the configuration above.

3. **Start GlueSync**:
   Run the GlueSync server to begin capturing and streaming events to Redis.

4. **Verify Data in Redis**:
   Use `redis-cli` or a Redis client to inspect the streams:
   ```bash
   redis-cli
   XREAD STREAMS gluesync_events:customers 0
   ```
   Events are stored as JSON with fields: `operation`, `payload`, `metadata`, and `before`.

## Example Event in Redis
For a table `customers`, an `UPDATE` event might look like:
```redis
XREAD STREAMS gluesync_events:customers 0
1) 1) "gluesync_events:customers"
   2) 1) 1) "1697051234567-0"
         2) 1) "event"
            2) "{\"operation\":\"UPDATE\",\"payload\":{\"id\":123,\"name\":\"John Smith\"},\"metadata\":{\"table\":\"customers\",\"database\":\"inventory\"},\"before\":{\"id\":123,\"name\":\"John Doe\"}}"
```

## Testing
1. **Unit Tests**:
   Create tests similar to `tests/test_connector.py` in the SDK repo. Mock `redis-py` and `ChangeEvent` objects to simulate event processing.

2. **Integration Tests**:
   - Set up a GlueSync source (e.g., PostgreSQL with sample data).
   - Configure the Redis sink with the above settings.
   - Perform `INSERT`, `UPDATE`, and `DELETE` operations on the source.
   - Verify events in Redis using `redis-cli` or a Python script.

3. **Sample Test Script**:
   ```python
   import redis
   r = redis.Redis(host="localhost", port=6379, decode_responses=True)
   events = r.xread({"gluesync_events:customers": "0"})
   for stream, messages in events:
       for msg_id, msg in messages:
           print(f"Message ID: {msg_id}, Event: {msg['event']}")
   ```

## Troubleshooting
- **Connection Errors**: Ensure Redis is running and accessible. Check `redis_host`, `redis_port`, and `redis_password` in the configuration.
- **Missing Events**: Verify the GlueSync source is capturing events correctly. Check logs for errors (configured via Python’s `logging`).
- **Performance Issues**: Adjust `redis_batch_size` to balance latency and throughput. Monitor Redis with `INFO` or `SLOWLOG`.
- **Schema Changes**: The connector logs warnings for unsupported operations. Extend the `write` method for custom schema handling if needed.

## Limitations
- **Synchronous Only**: The SDK is synchronous, limiting concurrency. Async support requires GlueSync SDK enhancements.
- **Schema Evolution**: Schema changes (e.g., new columns) are not explicitly handled. Downstream consumers must tolerate new fields.
- **Single Stream per Table**: Events are written to table-specific streams (e.g., `gluesync_events:customers`). Modify the connector for alternative structures (e.g., hashes).

## Contributing
Contributions are welcome! Please:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit changes (`git commit -m "Add feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a pull request.

## Support
For issues with the connector:
- Check the [GlueSync Documentation](https://docs.molo17.com/gluesync/v2.0/).
- Contact MOLO17 support at [https://www.gluesync.com/contact/](https://www.gluesync.com/contact/).
- File an issue in the repository (if public issues are enabled).

For GlueSync SDK questions, refer to the [SDK Repository](https://gitlab.com/molo17-public/gluesync/gluesync-python-corehub-handshake-sdk).

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

Copyright (c) 2025 Graziano Labs Corp. All rights reserved.
