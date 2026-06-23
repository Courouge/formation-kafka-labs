# Lab L3 — Producers & Consumers Scala

Voir `lab.md` pour le déroulé pédagogique complet.

## Prérequis

- JDK 17 (`java -version`)
- sbt 1.10+ (`sbt --version`)
- Cluster Docker du repo lancé : `docker compose up -d` à la racine du repo

## Lancer le code

Depuis `labs/L3-scala-producers-consumers/` :

```bash
sbt update                                  # télécharge les dépendances (Confluent, FS2, Avro4s)
sbt compile

# Étape 2 / 3 — kafka-clients impératif
sbt "runMain lab.ProducerSimple"
sbt "runMain lab.ConsumerSimple"

# Étape 4 / 5 — FS2-Kafka fonctionnel
sbt "runMain lab.ProducerFs2"
sbt "runMain lab.ConsumerFs2"

# Étape 6 — Avro4s + Schema Registry
sbt "runMain lab.ProducerAvro"
sbt "runMain lab.ConsumerAvro"
```

## Variables d'environnement

| Variable               | Défaut                                         |
|------------------------|------------------------------------------------|
| `BOOTSTRAP_SERVERS`    | `localhost:9092,localhost:9093,localhost:9094` |
| `SCHEMA_REGISTRY_URL`  | `http://localhost:8081`                        |
| `TOPIC`                | `orders.scala`                                 |
| `TOPIC_AVRO`           | `orders.scala.avro`                            |
| `GROUP_ID`             | `scala-consumer`                               |

## Création des topics

```bash
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic orders.scala --partitions 3 --replication-factor 3
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic orders.scala.avro --partitions 3 --replication-factor 3
```

## Solutions

Les versions complètes (avec producer transactionnel, exactly-once, tests
ScalaTest) sont dans `solutions/L3-scala-producers-consumers/`.
