# JMX Prometheus Exporter — agent Java

Le fichier `kafka-jmx-exporter.yml` est la configuration des règles JMX -> Prometheus.
L'agent Java (`jmx_prometheus_javaagent.jar`) doit être présent dans ce dossier
avant de lancer `docker compose up`. Il est monté en lecture seule dans chaque broker
Kafka via `./jmx:/opt/jmx:ro` et chargé par `KAFKA_OPTS=-javaagent:...`.

## Téléchargement

```bash
# Depuis la racine du repo
JMX_VERSION=0.20.0
curl -fL -o docker/jmx/jmx_prometheus_javaagent.jar \
  "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar"
```

## Vérification

Après `docker compose up -d`, les métriques sont disponibles sur :

- `http://localhost:7071/metrics` (kafka1)
- `http://localhost:7072/metrics` (kafka2)
- `http://localhost:7073/metrics` (kafka3)

Et scrapées automatiquement par Prometheus (`prometheus:9090`).
