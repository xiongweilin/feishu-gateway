from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI

_INSTRUMENTED = False


def configure_telemetry(app: FastAPI, service_name: str = "feishu-gateway") -> bool:
    """Attach OTLP tracing when an OTLP endpoint is configured.

    Without OTEL_EXPORTER_OTLP_ENDPOINT this is a no-op, so local runs, tests and
    CI behave exactly as before. Returns True when instrumentation was applied.
    """
    global _INSTRUMENTED
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        return False
    # OTEL_EXPORTER_OTLP_ENDPOINT is a base URL by spec, but the explicit
    # exporter constructor does not append the signal path the way the SDK's
    # env-var handling does.
    endpoint = endpoint.rstrip("/")
    if not endpoint.endswith("/v1/traces"):
        endpoint = f"{endpoint}/v1/traces"
    if _INSTRUMENTED:
        return True

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(
        {SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", service_name)}
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    _INSTRUMENTED = True
    return True
