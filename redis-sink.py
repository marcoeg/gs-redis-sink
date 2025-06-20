import redis
import json
import logging
from gluesync_sdk.connector import BaseConnector, ConnectorError
from gluesync_sdk.models import ChangeEvent
from typing import List, Dict, Any

class RedisSinkConnector(BaseConnector):
    def __init__(self):
        # Initialize the base connector from the GlueSync SDK.
        # Question: Does BaseConnector provide any built-in logging or metrics setup
        # that should be configured here? The SDK documentation (src/gluesync_sdk/connector.py)
        # does not specify initialization requirements beyond super().__init__().
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.redis_client = None
        self.stream_name = None
        self.key_prefix = None
        self.batch_size = None

    def connect(self, config: Dict[str, Any]) -> None:
        """Initialize Redis connection and validate configuration.

        Args:
            config: Dictionary containing Redis connection and sink settings.

        Raises:
            ConnectorError: If configuration is invalid or Redis connection fails.

        Notes:
            - Validates required keys based on src/gluesync_sdk/config.py's ConnectorConfig approach.
            - Uses redis-py for Redis interactions, configured with retry and connection pooling.
            - Question: Does the SDK provide a standard way to validate config keys or schema?
              The documentation (src/gluesync_sdk/config.py) only shows basic validation,
              and it's unclear if custom keys like redis_stream_name require registration.
            - Question: Are there SDK-specific retry or timeout settings that should override
              redis-py's retry_on_timeout? The SDK docs lack details on connection management.
        """
        try:
            # Validate required configuration keys
            required_keys = ["redis_host", "redis_port"]
            for key in required_keys:
                if key not in config:
                    raise ConnectorError(f"Missing required config key: {key}")

            # Load configuration with defaults
            self.redis_host = config.get("redis_host", "localhost")
            self.redis_port = config.get("redis_port", 6379)
            self.redis_password = config.get("redis_password", None)
            self.stream_name = config.get("redis_stream_name", "gluesync_events")
            self.key_prefix = config.get("redis_key_prefix", "gluesync:")
            self.batch_size = config.get("redis_batch_size", 100)
            self.ssl_enabled = config.get("redis_ssl", False)

            # Initialize redis-py client with connection pooling and retries
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                decode_responses=True,
                ssl=self.ssl_enabled,
                retry_on_timeout=True,
                max_connections=10
            )

            # Test Redis connection
            self.redis_client.ping()
            self.logger.info(
                f"Connected to Redis at {self.redis_host}:{self.redis_port}, stream: {self.stream_name}"
            )
        except redis.RedisError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise ConnectorError(f"Redis connection failed: {e}")
        except Exception as e:
            self.logger.error(f"Configuration error: {e}")
            raise ConnectorError(f"Invalid configuration: {e}")

    def write(self, event: ChangeEvent) -> None:
        """Process a single CDC event and write to Redis Stream.

        Args:
            event: ChangeEvent object containing operation, payload, and metadata.

        Raises:
            ConnectorError: If event is invalid or Redis write fails.

        Notes:
            - Uses Redis Streams for event storage, leveraging XADD for append-only logs.
            - Constructs stream key using metadata.table to organize events by source table.
            - Serializes full event (operation, payload, metadata, before) to JSON.
            - Question: How does the SDK handle schema changes (e.g., new fields in payload)?
              src/gluesync_sdk/models.py defines ChangeEvent but lacks schema evolution details.
            - Question: Are operation values always INSERT, UPDATE, DELETE, or are there
              other types (e.g., TRUNCATE) that need handling? The SDK docs don’t specify.
            - Issue: If metadata.table is missing, defaults to 'unknown'. Should the connector
              fail fast or use a fallback? This is not addressed in the SDK documentation.
        """
        try:
            operation = event.operation
            payload = event.after if event.after is not None else event.before
            if not payload or "id" not in payload:
                raise ConnectorError(f"Invalid event payload, missing 'id': {event}")

            event_id = payload["id"]
            table = event.metadata.get("table", "unknown")
            stream_key = f"{self.stream_name}:{table}"

            if operation in ["INSERT", "UPDATE"]:
                # Serialize event with metadata for downstream consumers
                event_dict = {
                    "operation": operation,
                    "payload": payload,
                    "metadata": event.metadata,
                    "before": event.before
                }
                event_json = json.dumps(event_dict)
                self.redis_client.xadd(stream_key, {"event": event_json})
                self.logger.debug(f"Wrote {operation} event to stream {stream_key}: {event_id}")
            elif operation == "DELETE":
                # Append tombstone event for deletes
                tombstone = {
                    "operation": "DELETE",
                    "id": event_id,
                    "metadata": event.metadata
                }
                self.redis_client.xadd(stream_key, {"event": json.dumps(tombstone)})
                self.logger.debug(f"Wrote DELETE event to stream {stream_key}: {event_id}")
            else:
                self.logger.warning(f"Unsupported operation {operation}, skipping event: {event_id}")
        except redis.RedisError as e:
            self.logger.error(f"Failed to write event to Redis: {event}, error: {e}")
            raise ConnectorError(f"Redis write failed: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to serialize event to JSON: {event}, error: {e}")
            raise ConnectorError(f"JSON serialization failed: {e}")

    def batch_write(self, events: List[ChangeEvent]) -> None:
        """Process a batch of CDC events using Redis pipelining.

        Args:
            events: List of ChangeEvent objects to write.

        Raises:
            ConnectorError: If batch write or serialization fails.

        Notes:
            - Uses Redis pipelining to optimize throughput for batch writes.
            - Skips invalid events to prevent pipeline failure but logs warnings.
            - Question: Does the SDK enforce a maximum batch size for batch_write?
              src/gluesync_sdk/connector.py doesn’t specify, impacting performance tuning.
            - Question: How does the SDK handle backpressure if Redis is slow?
              No backpressure mechanism is documented, which could affect high-throughput scenarios.
            - Issue: Partial batch failures (e.g., one event fails) are not retried individually.
              The SDK doesn’t provide guidance on partial failure handling.
        """
        if not events:
            self.logger.debug("Empty batch, skipping")
            return

        try:
            pipeline = self.redis_client.pipeline()
            for event in events:
                operation = event.operation
                payload = event.after if event.after is not None else event.before
                if not payload or "id" not in payload:
                    self.logger.warning(f"Invalid event payload, missing 'id': {event}")
                    continue

                event_id = payload["id"]
                table = event.metadata.get("table", "unknown")
                stream_key = f"{self.stream_name}:{table}"

                if operation in ["INSERT", "UPDATE"]:
                    event_dict = {
                        "operation": operation,
                        "payload": payload,
                        "metadata": event.metadata,
                        "before": event.before
                    }
                    event_json = json.dumps(event_dict)
                    pipeline.xadd(stream_key, {"event": event_json})
                elif operation == "DELETE":
                    tombstone = {
                        "operation": "DELETE",
                        "id": event_id,
                        "metadata": event.metadata
                    }
                    pipeline.xadd(stream_key, {"event": json.dumps(tombstone)})
                else:
                    self.logger.warning(f"Unsupported operation {operation}, skipping event: {event_id}")

            pipeline.execute()
            self.logger.info(f"Wrote {len(events)} events to Redis stream")
        except redis.RedisError as e:
            self.logger.error(f"Failed to batch write events to Redis: {e}")
            raise ConnectorError(f"Redis batch write failed: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to serialize events to JSON: {e}")
            raise ConnectorError(f"JSON serialization failed: {e}")

    def commit(self) -> None:
        """Confirm successful writes (offsets managed by GlueSync).

        Notes:
            - Currently a no-op, assuming GlueSync handles offset tracking internally.
            - Question: Does the SDK require the connector to track offsets (e.g., Redis Stream IDs)?
              src/gluesync_sdk/connector.py suggests offsets are managed by GlueSync,
              but confirmation is needed for exactly-once delivery scenarios.
        """
        self.logger.debug("Commit called")

    def disconnect(self) -> None:
        """Clean up Redis connection.

        Raises:
            ConnectorError: If disconnection fails.

        Notes:
            - Closes the redis-py connection to release resources.
            - Question: Does the SDK expect disconnect to handle partial connection states
              (e.g., pipeline in progress)? The documentation doesn’t specify cleanup requirements.
        """
        try:
            if self.redis_client:
                self.redis_client.close()
                self.logger.info("Disconnected from Redis")
        except redis.RedisError as e:
            self.logger.error(f"Error during Redis disconnect: {e}")
            raise ConnectorError(f"Redis disconnect failed: {e}")