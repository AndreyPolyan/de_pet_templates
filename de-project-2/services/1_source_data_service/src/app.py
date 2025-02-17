from flask import Flask, jsonify, request
import logging
import redis
from app_config import AppConfig
from loader.processor import StgMessageGenerator

# Configure logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

app = Flask(__name__)

config = AppConfig()

# Initialize message generator
message_generator = StgMessageGenerator(config.kafka_producer(), config.redis_client(), log)

@app.route("/health", methods=["GET"])
def health_check():
    """API Health Check."""
    return jsonify({"status": "ok"})

@app.route("/push", methods=["POST"])
def push_messages():
    """Trigger message generation manually."""
    try:
        data = request.get_json()
        num_messages = int(data.get("N", 1))  # Default: 1 message if not specified

        if num_messages <= 0:
            return jsonify({"error": "N must be greater than 0"}), 400

        message_generator.push_messages(num_messages)
        return jsonify({"status": "success", "messages_pushed": num_messages}), 200

    except Exception as e:
        log.error(f"Error pushing messages: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    log.info("Starting Flask API...")
    app.run(host="0.0.0.0", port=5001)