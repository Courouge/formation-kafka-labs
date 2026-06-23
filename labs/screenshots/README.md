# Screenshots de progression

Ce dossier peut contenir les captures prises pendant les labs. Les screenshots ne sont pas nécessaires au fonctionnement du code, mais ils aident à vérifier la compréhension.

Voir la liste complète dans ../../modules/B1-M06/OBSERVABILITE_LABS.md.

## Noms recommandés

```text
01-kafka-ui-brokers.png
02-grafana-brokers-up.png
03-topic-orders-events.png
04-consumer-group-offsets.png
05-messages-in-rate.png
06-schema-registry-ordercreated.png
07-dlq-message.png
08-connect-debezium-running.png
09-cdc-topic-events.png
10-connect-records-rate.png
11-broker-failure-urp.png
12-minio-bronze-files.png
```

## Règle

Une bonne capture doit montrer :

- le nom de l'outil ou du dashboard ;
- le nom du topic, consumer group, connector ou bucket observé ;
- une valeur qui prouve l'état attendu : `RUNNING`, `lag=0`, `URP=0`, messages présents, fichier Bronze créé.
