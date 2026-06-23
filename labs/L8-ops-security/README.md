# Lab L8 — Ops & Sécurité

Voir [`lab.md`](./lab.md) pour le déroulé complet.

## Démarrage rapide

### Partie 1 — Observabilité (cluster L1 plaintext)

Pré-requis : la stack du lab L1 doit tourner (`make up` depuis `labs/L1-setup`).

```bash
cd labs/L8-ops-security
make metrics-check        # vérifie que JMX exporter répond sur 7071/7072/7073
make prom-targets         # statut des targets Prometheus
make dashboard-install    # vérifie les dashboards Grafana provisionnés
make alerts-install       # règles d'alerte Prometheus de base

# Provoquer un incident pour observer Grafana :
make traffic-on           # 1k msg/s en arrière-plan
make break-broker         # docker stop kafka2 -> URP monte
make restore-broker       # docker start kafka2 -> ISR se reconstruit
make traffic-off
```

### Partie 2 — Sécurité (cluster séparé SASL/SCRAM)

```bash
cd labs/L8-ops-security
make sec-up               # démarre kafka-sec-1/2/3 sur 19092/3/4
make users                # crée producer-app et consumer-app (SCRAM)
make acls                 # crée le topic orders + ACLs

# Tests Python (nécessite confluent-kafka==2.3.0)
pip install confluent-kafka==2.3.0
make test-allowed         # producer-app produit, consumer-app consomme
make test-denied          # tente des opérations refusées (ACL)

# Nettoyage
make sec-down             # stoppe (garde les volumes)
make sec-clean            # stoppe + détruit les volumes
```

## Arborescence

```
labs/L8-ops-security/
├── lab.md                          # support pédagogique
├── Makefile                        # cibles d'orchestration
├── docker-compose.security.yml     # cluster Kafka SASL/SCRAM (3 brokers KRaft)
├── users/
│   ├── admin.properties            # config client SASL pour les commandes admin
│   └── setup_users.sh              # crée producer-app + consumer-app
├── acls/
│   └── setup_acls.sh               # ACLs producer/consumer + topic orders
├── prometheus/
│   └── rules.yml                   # alertes URP, controllers, broker down
├── clients/
│   ├── producer_sasl.py            # producer Python authentifié
│   ├── consumer_sasl.py            # consumer Python authentifié
│   └── producer_unauthorized.py    # démontre des refus ACL
└── incidents/
    └── break_broker.sh             # outils incident (trafic + stop broker)
```

## Ports

| Service | Port hôte | Stack |
|---|---|---|
| Brokers L1 (plaintext) | 9092 / 9093 / 9094 | L1 (`docker/docker-compose.yml`) |
| JMX exporter L1 | 7071 / 7072 / 7073 | L1 |
| Prometheus | 9090 | L1 |
| Grafana | 13000 | L1 |
| Brokers SASL/SCRAM | 19092 / 19093 / 19094 | L8 (`docker-compose.security.yml`) |
| JMX exporter sécurisé | 17071 / 17072 / 17073 | L8 |
