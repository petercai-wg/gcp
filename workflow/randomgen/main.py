import functions_framework
import random
from flask import jsonify


@functions_framework.http
def randomgen(request):
    randomNum = random.randint(1, 100)
    output = {"random": randomNum}
    print(f'cloud function generated ${randomNum} ')
    return jsonify(output)
