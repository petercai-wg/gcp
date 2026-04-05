from flask import Flask, request
import random
import time

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health_check():
    return "health check OK \n", 200


@app.route("/", methods=["POST"])
def handle_task():
    data = request.json

    print(f"Processing a task: {data}")

    # simulate slow work
    time.sleep(random.randint(1, 5))

    # simulate fail to retry task
    if random.random() < 0.3:
        print("Task failed")
        return "Task failed", 500

    print("Task completed")
    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
