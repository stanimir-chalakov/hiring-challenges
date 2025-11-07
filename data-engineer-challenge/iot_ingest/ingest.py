import os
import json
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from utils.logger import setup_logger, colorize_message

logger = setup_logger("iot_ingest")

class IoTIngestor:
    def __init__(self):
        # Environment configuration
        self.mqtt_host = os.getenv("MQTT_HOST", "mqtt_broker")
        self.mqtt_port = int(os.getenv("MQTT_PORT", 1883))
        self.mqtt_topic = os.getenv("MQTT_TOPIC", "sensors")
        self.local_path = os.getenv("LOCAL_PATH", "/data")

        # MQTT client setup
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self._last_file_path = None


    def on_connect(self, client, userdata, flags, rc):
        """Callback when MQTT connects."""
        logger.info(colorize_message(f"[Ingest] Connected to MQTT broker at {self.mqtt_host}:{self.mqtt_port} (code {rc})"))
        client.subscribe(self.mqtt_topic)
        logger.info(colorize_message(f"[Ingest] Subscribed to topic: {self.mqtt_topic}"))

    def on_message(self, client, userdata, msg):
        """Callback for new messages."""
        try:
            payload = msg.payload.decode()

            # Use a per-minute filename (UTC) so all readings in the same minute go to one file.
            minute_key = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

            os.makedirs(self.local_path, exist_ok=True)
            file_path = os.path.join(self.local_path, f"msgs_{minute_key}.json")

            # Normalize to a single-line JSON entry per message so file is JSON Lines compatible.
            try:
                parsed = json.loads(payload)
                line = json.dumps(parsed, separators=(",", ":"))
            except json.JSONDecodeError:
                # store raw payload as a JSON string to keep lines valid JSON
                line = json.dumps(payload)

            with open(file_path, "a") as f:
                f.write(line + "\n")
            if file_path != self._last_file_path:
                logger.info(colorize_message(f"[Ingest] Appended message to → {file_path}"))
                self._last_file_path = file_path

        except Exception as e:
            logger.error(colorize_message(f"[Ingest] Error processing message: {e}"))

    def run(self):
        """Start MQTT client loop."""
        logger.info(colorize_message("[Ingest] Starting MQTT ingestion service..."))
        try:
            self.client.connect(self.mqtt_host, self.mqtt_port)
        except Exception as e:
            logger.error(colorize_message(f"[Ingest] Could not connect to MQTT broker: {e}"))
            raise

        # start network loop (blocking)
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info(colorize_message("[Ingest] Keyboard interrupt received, stopping"))
        except Exception as e:
            logger.error(colorize_message(f"[Ingest] MQTT loop error: {e}"))


if __name__ == "__main__":
    ingestor = IoTIngestor()
    ingestor.run()
