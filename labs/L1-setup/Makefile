# Makefile Lab L1 — orchestration de la stack minimale L1
# Compose local au lab : 3 brokers KRaft + Schema Registry + Kafka UI.
# Les labs L2+ basculent sur le compose complet docker/docker-compose.yml a la racine.

COMPOSE      := docker compose -f docker-compose.yml
BOOTSTRAP    := kafka1:29092

.PHONY: help up down logs ps restart topics-list health clean

help:
	@echo "Cibles disponibles :"
	@echo "  make up           - Démarrer le cluster L1 (3 brokers + SR + Kafka UI)"
	@echo "  make down         - Arrêter la stack (volumes conservés)"
	@echo "  make clean        - Arrêter et supprimer les volumes (perte de données)"
	@echo "  make logs         - Suivre les logs (Ctrl-C pour quitter)"
	@echo "  make ps           - Lister les conteneurs"
	@echo "  make restart      - Redémarrer la stack"
	@echo "  make topics-list  - Lister les topics du cluster"
	@echo "  make health       - Vérifier brokers, Schema Registry et Kafka UI"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

restart:
	$(COMPOSE) restart

topics-list:
	docker exec kafka1 kafka-topics --bootstrap-server $(BOOTSTRAP) --list

health:
	@echo "--- Conteneurs ---"
	@$(COMPOSE) ps --format "table {{.Service}}\t{{.State}}\t{{.Status}}"
	@echo ""
	@echo "--- Brokers (kafka-broker-api-versions) ---"
	@for b in kafka1 kafka2 kafka3; do \
		printf "%-8s : " $$b; \
		docker exec $$b kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1 \
			&& echo "OK" || echo "KO"; \
	done
	@echo ""
	@echo "--- Quorum KRaft (controller leader, voters, observers) ---"
	@docker exec kafka1 kafka-metadata-quorum --bootstrap-server localhost:29092 describe --status 2>/dev/null \
		|| echo "quorum         : KO (cluster pas encore pret ?)"
	@echo ""
	@echo "--- Schema Registry ---"
	@curl -fsS http://localhost:8081/subjects >/dev/null && echo "schema-registry : OK  (http://localhost:8081)" || echo "schema-registry : KO"
	@echo ""
	@echo "--- Kafka UI ---"
	@curl -fsS http://localhost:18080/ >/dev/null && echo "kafka-ui        : OK  (http://localhost:18080)" || echo "kafka-ui        : KO"
