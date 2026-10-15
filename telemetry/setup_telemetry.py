from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_telemetry(service_name: str, otlp_endpoint: str) -> None:
    """
    Настройка OpenTelemetry: traces/metrics/logs → OTLP-коллектор (gRPC, insecure).

    :param service_name: имя сервиса для ресурса телеметрии
    :param otlp_endpoint: адрес OTLP-коллектора
    """
    resource = Resource.create({SERVICE_NAME: service_name})

    # Traces: batch-процессор буферизует спаны и экспортирует пачками.
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True),
            max_queue_size=2048,
            max_export_batch_size=512,
            schedule_delay_millis=1000,
        )
    )
    trace.set_tracer_provider(tracer_provider)

    # Metrics: push-модель через периодический экспорт каждые 10 секунд.
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
                export_interval_millis=10_000,
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    # Logs: отдельный провайдер, связанный со стандартным logging ниже.
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=otlp_endpoint, insecure=True))
    )
    set_logger_provider(logger_provider)

    # Связываем стандартный logging с OTel: trace_id/span_id попадают в записи.
    LoggingInstrumentor().instrument(set_logging_format=True)


def shutdown_telemetry() -> None:
    """Корректная остановка провайдеров (flush накопленных спанов/метрик)."""
    trace.get_tracer_provider().shutdown()
    metrics.get_meter_provider().shutdown()
