import os
import socket
import time


STATSD_HOST = os.getenv("AIRFLOW__METRICS__STATSD_HOST", "statsd-exporter")
STATSD_PORT = int(os.getenv("AIRFLOW__METRICS__STATSD_PORT", "9125"))


def send_metric(name, value, metric_type):
    """Send a metric to StatsD without interrupting the pipeline."""

    message = f"{name}:{value}|{metric_type}"

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(
                message.encode("utf-8"),
                (STATSD_HOST, STATSD_PORT)
            )
    except OSError:
        pass


class PipelineMetrics:

    def __init__(self, stage, operation):
        self.prefix = f"retailpulse.{stage}.{operation}"
        self.start_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def record_records(self, count):
        send_metric(
            f"{self.prefix}.records",
            count,
            "c"
        )

    def __exit__(self, exc_type, exc_value, traceback):

        duration_ms = (
            time.perf_counter() - self.start_time
        ) * 1000

        send_metric(
            f"{self.prefix}.duration_ms",
            duration_ms,
            "ms"
        )

        status = "failure" if exc_type else "success"

        send_metric(
            f"{self.prefix}.runs_{status}",
            1,
            "c"
        )

        return False
