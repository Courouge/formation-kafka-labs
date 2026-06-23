#!/usr/bin/env bash
# Génère quelques événements CDC en faisant des UPDATE sur Postgres.
# Usage : ./scripts/make-update.sh
set -euo pipefail

CONTAINER="${POSTGRES_CONTAINER:-postgres}"

echo "Génération d'événements CDC dans Postgres (container ${CONTAINER})..."

docker exec -e PGPASSWORD=postgres "${CONTAINER}" psql -U postgres -d ecommerce -v ON_ERROR_STOP=1 <<'SQL'
-- UPDATE customers : changement d'email + recalcul updated_at
UPDATE customers
SET email = 'alice.new+' || extract(epoch from now())::bigint || '@example.com',
    updated_at = NOW()
WHERE customer_id = 1;

-- UPDATE orders : transition d'état pending -> paid
UPDATE orders
SET status = 'paid',
    updated_at = NOW()
WHERE order_id = 3 AND status = 'pending';

-- INSERT order_items pour générer un nouveau message CDC sur cette table
INSERT INTO order_items (order_id, sku, product_name, quantity, unit_price)
VALUES (1, 'SKU-CDC-DEMO', 'Item demo CDC', 1, 9.99);

-- DELETE pour démontrer la gestion des suppressions par la SMT unwrap
DELETE FROM order_items WHERE sku = 'SKU-CDC-DEMO';

SELECT 'OK : 4 événements CDC générés (1 UPDATE customer, 1 UPDATE order, 1 INSERT item, 1 DELETE item)' AS msg;
SQL
