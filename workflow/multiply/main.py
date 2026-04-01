import time

import functions_framework
from flask import jsonify
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


@functions_framework.http
def multiply(request):
    request_json = request.get_json()
    output = {"multiplied": 2.33 * request_json["input"]}
    # Sleep for a random time to imitate a random processing time
    time.sleep(random.uniform(0, 0.5))
    print(f"multiple from {request_json['input']} to {output} ")
    return jsonify(output)
