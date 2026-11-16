from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge
import json

failure_score = Gauge(
    "server_failure_score",
    "Failure score",
    ["server_id"]
)
server_status = Gauge(
    "server_failure_status",
    "1 = failure likely, 0 = healthy",
    ["server_id"]
)

cpu_usage = Gauge(
    "server_cpu_usage",
    "CPU usage",
    ["server_id"]
)

memory_usage = Gauge(
    "server_memory_usage",
    "Memory usage",
    ["server_id"]
)

disk_usage = Gauge(
    "server_disk_usage",
    "Disk usage",
    ["server_id"]
)

response_time = Gauge(
    "server_response_time",
    "Response time",
    ["server_id"]
)

error_count = Gauge(
    "server_error_count",
    "Error count",
    ["server_id"]
)

consumer = KafkaConsumer(
    "failure-predictions",
    bootstrap_servers="kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest"
)

start_http_server(8000)

print("Metrics exporter started")

for msg in consumer:
    data = msg.value

    server_id = data["server_id"]

    cpu_usage.labels(server_id=server_id).set(
        data["cpu"]
    )

    memory_usage.labels(server_id=server_id).set(
        data["memory"]
    )

    disk_usage.labels(server_id=server_id).set(
        data["disk"]
    )

    response_time.labels(server_id=server_id).set(
        data["response_time"]
    )

    error_count.labels(server_id=server_id).set(
        data["error_count"]
    )

    failure_score.labels(server_id=server_id).set(
        data["failure_score"]
    )

    server_status.labels(server_id=server_id).set(
        1 if data["status"] == "FAILURE_LIKELY" else 0
    )
