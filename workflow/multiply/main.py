import functions_framework
from flask import jsonify


@functions_framework.http
def multiply(request):
    request_json = request.get_json()
    output = {"multiplied": 2 * request_json['input']}
    print(f"multiple from {request_json['input']} to {output} ")
    return jsonify(output)
