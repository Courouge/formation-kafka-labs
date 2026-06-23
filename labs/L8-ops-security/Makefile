# Makefile Lab L8 — Ops & Sécurité
#
# Cibles :
#   Observabilité (cluster L1) :
#     metrics-check       : curl /metrics sur les 3 brokers L1
#     prom-targets        : statut des targets Prometheus
#     alerts-install      : copie rules.yml + patch prometheus.yml + reload
#     traffic-on          : démarre un producer perf-test sur events.demo
#     traffic-off         : stoppe le perf-test
#     break-broker        : docker stop kafka2 (incident)
#     restore-broker      : docker start kafka2
#     dashboard-install   : vérifie les dashboards Grafana provisionnés
#
#   Sécurité (cluster séparé sur 19092/3/4) :
#     sec-up              : démarre la stack sécurisée
#     sec-down            : stoppe la stack (volumes conservés)
#     sec-clean           : stoppe + supprime volumes (perte des SCRAM users)
#     sec-ps              : liste des conteneurs
#     sec-logs            : logs en suivi
#     users               : crée producer-app et consumer-app (SCRAM)
#     acls                : crée le topic orders et applique les ACLs
#     test-allowed        : producer + consumer Python autorisés
#     test-denied         : démontre 2 refus d'autorisation

SHELL := /usr/bin/env bash
SEC_COMPOSE := docker compose -f docker-compose.security.yml
PROM_CONFIG := ../../docker/prometheus/prometheus.yml
PROM_RULES_DIR := ../../docker/prometheus/rules
GRAFANA_DASH_DIR := ../../docker/grafana/dashboards

.PHONY: help \
        metrics-check prom-targets alerts-install \
        traffic-on traffic-off break-broker restore-broker \
        dashboard-install \
        sec-up sec-down sec-clean sec-ps sec-logs \
        users acls test-allowed test-denied

help:
	@grep -E '^# *' Makefile | head -40

# ---------------------------------------------------------------------------
# Partie 1 : observabilité
# ---------------------------------------------------------------------------

metrics-check:
	@for p in 7071 7072 7073; do \
		echo "--- broker port $$p ---" ; \
		curl -fsS http://localhost:$$p/metrics | grep -E "^kafka_server_brokertopicmetrics_(messagesinpersec|bytesinpersec)" | head -3 || echo "KO" ; \
	done

prom-targets:
	@curl -fsS http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, instance: .labels.instance, health: .health}'

alerts-install:
	@mkdir -p $(PROM_RULES_DIR)
	@cp prometheus/rules.yml $(PROM_RULES_DIR)/rules.yml
	@if ! grep -q "rule_files:" $(PROM_CONFIG); then \
		printf '\nrule_files:\n  - /etc/prometheus/rules/*.yml\n' >> $(PROM_CONFIG) ; \
		echo "[alerts] rule_files ajouté à prometheus.yml" ; \
	fi
	@if ! docker inspect prometheus --format '{{range .Mounts}}{{.Destination}} {{end}}' | grep -q "/etc/prometheus/rules"; then \
		echo "[alerts] ATTENTION : le dossier rules/ n'est pas monté dans le conteneur Prometheus." ; \
		echo "         Ajoute le volume suivant à docker/docker-compose.yml service prometheus :" ; \
		echo "           - ./prometheus/rules:/etc/prometheus/rules:ro" ; \
		echo "         Puis : docker compose -f ../../docker/docker-compose.yml up -d prometheus" ; \
	fi
	@curl -s -X POST http://localhost:9090/-/reload && echo "[alerts] reload Prometheus OK" || echo "[alerts] reload KO (vérifier --web.enable-lifecycle)"

traffic-on:
	@bash incidents/break_broker.sh traffic-on

traffic-off:
	@bash incidents/break_broker.sh traffic-off

break-broker:
	@bash incidents/break_broker.sh break

restore-broker:
	@bash incidents/break_broker.sh restore

dashboard-install:
	@test -f $(GRAFANA_DASH_DIR)/kafka-learning.json
	@test -f $(GRAFANA_DASH_DIR)/kafka-cluster.json
	@test -f $(GRAFANA_DASH_DIR)/kafka-connect.json
	@echo "[dashboard] dashboards provisionnés : Kafka Learning Dashboard, Kafka Cluster, Kafka Connect."
	@echo "[dashboard] ouvrir http://localhost:13000 puis dossier Formation Kafka."

# ---------------------------------------------------------------------------
# Partie 2 : sécurité
# ---------------------------------------------------------------------------

sec-up:
	$(SEC_COMPOSE) up -d
	@echo "[sec] attente 20s pour quorum..."
	@sleep 20
	@$(SEC_COMPOSE) ps

sec-down:
	$(SEC_COMPOSE) down

sec-clean:
	$(SEC_COMPOSE) down -v

sec-ps:
	$(SEC_COMPOSE) ps

sec-logs:
	$(SEC_COMPOSE) logs -f --tail=100

users:
	@bash users/setup_users.sh

acls:
	@bash acls/setup_acls.sh

test-allowed:
	@python clients/producer_sasl.py
	@python clients/consumer_sasl.py

test-denied:
	@python clients/producer_unauthorized.py
