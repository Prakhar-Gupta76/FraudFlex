# Local Kafka Broker

FraudFlux uses one Apache Kafka broker/controller in KRaft combined mode for
local MVP development. ZooKeeper is not used.

## Prerequisites

- Docker Desktop with Docker Compose
- Docker version 20.10.4 or newer
- Approximately 1 GB of available Docker memory
- Port `9092` available on localhost

The broker uses the official `apache/kafka:4.3.1` image. The client listener is
bound to `127.0.0.1:9092` and uses plaintext communication, so this
configuration is for local development only.

## Start Kafka and create topics

From the repository root:

```powershell
docker compose up -d
```

The `kafka-init` container waits for the broker health check and then creates
all four topics. It exits successfully after initialization.

Kafka data is persisted at `/var/lib/kafka/data`, the writable directory
owned by the official image's non-root `appuser`. Mounting the named volume at
a root-owned directory prevents KRaft from writing its bootstrap metadata on
some Docker Desktop installations.

Inspect status and initialization logs:

```powershell
docker compose ps
docker compose logs kafka-init
```

Check the broker and required topics from Python after installing project
dependencies:

```powershell
pip install -e .
fraudflux-kafka-check
```

## Stop and restart

Stopping or recreating the broker preserves acknowledged records in the named
volume:

```powershell
docker compose stop kafka
docker compose start kafka
```

Stop the complete environment:

```powershell
docker compose down
```

Do not add `--volumes` unless you intentionally want to erase all local Kafka
data.

## Inspect topics

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe
```

Inspect scored decisions or actionable alerts:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic transactions.scored `
  --from-beginning

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic fraud.alerts `
  --from-beginning
```

The scoring worker uses validated schema `1.0` events, customer IDs as message
keys, and synchronous delivery confirmation. Every completed decision appears
in `transactions.scored`; only medium- and high-risk decisions appear in
`fraud.alerts`.

## Live integration tests

Once the broker is healthy:

```powershell
$env:FRAUDFLUX_RUN_KAFKA_INTEGRATION = "1"
python -m unittest discover -s tests -v
Remove-Item Env:FRAUDFLUX_RUN_KAFKA_INTEGRATION
```

These tests publish and consume real records. They are skipped during ordinary
unit-test runs.

## MVP limitations

- One broker means no broker failover.
- Replication factor is 1.
- Authentication and TLS are not configured.
- Retention limits are intentionally small for an 8 GB development machine.
