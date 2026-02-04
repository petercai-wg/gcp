from flask import Flask, render_template
import platform
import os

# Create a Flask application
app = Flask(__name__)

appType = os.getenv("APPTYPE", "default")


# Define a route for the root URL
@app.route("/")
def hello_world():
    hostname = platform.node()
    data = {
        "title": "Dynamic Content from Server",
        "items": ["Item A", "Item B", "Item C"],
    }

    return render_template(
        "index.html", hostname=hostname, appType=appType, server_data=data
    )


# Run the Flask application if this file is executed directly
if __name__ == "__main__":
    app.run(debug=True, port=9080, host="0.0.0.0")
