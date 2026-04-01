import functions_framework

from flask import jsonify

import math
import random

from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.cloud_trace_propagator import CloudTraceFormatPropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_exporter(exporter):
    """Configures OpenTelemetry context propagation to use Cloud Trace context
    exporter: exporter instance to be configured in the OpenTelemetry tracer provider
    """
    set_global_textmap(CloudTraceFormatPropagator())
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(tracer_provider)


configure_exporter(CloudTraceSpanExporter())
tracer = trace.get_tracer(__name__)


def is_prime(n):
    """Checks if a single number 'n' is a prime number."""
    if n <= 1:
        return False
    # Check for factors from 2 up to the square root of n
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True


def generate_primes_in_range(start, end):
    """Generates all prime numbers within a given range."""
    primes = []
    for num in range(start, end + 1):
        if is_prime(num):
            primes.append(num)
    return primes


@functions_framework.http
def randomgen(request):
    primeNumbers = generate_primes_in_range(1, 100)
    r = random.randint(1, len(primeNumbers))
    randomNum = primeNumbers[r - 1]

    output = {"random": randomNum}
    print(f"cloud function generated {randomNum} ")
    return jsonify(output)
