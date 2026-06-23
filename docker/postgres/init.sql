-- Initialisation du schéma e-commerce pour les labs Kafka Connect / Debezium.
-- Exécuté automatiquement par l'image postgres:17-alpine au premier démarrage.

-- Patch test-labs: tables créées dans `public` pour matcher la conf Debezium
-- (qui pointe public.customers/orders/order_items). Le schéma ecommerce restait inutilisé.
SET search_path TO public;

-- Table customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id   SERIAL PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    country       VARCHAR(2)   NOT NULL DEFAULT 'FR',
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Table orders
CREATE TABLE IF NOT EXISTS orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INTEGER NOT NULL REFERENCES customers(customer_id),
    status        VARCHAR(20) NOT NULL CHECK (status IN ('pending','paid','shipped','delivered','cancelled')),
    total_amount  NUMERIC(10,2) NOT NULL,
    currency      VARCHAR(3) NOT NULL DEFAULT 'EUR',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status   ON orders(status);

-- Table order_items
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    sku           VARCHAR(50) NOT NULL,
    product_name  VARCHAR(255) NOT NULL,
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10,2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);

-- REPLICA IDENTITY FULL pour Debezium (capture des anciennes valeurs sur UPDATE/DELETE)
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Création d'une publication Postgres pour Debezium
CREATE PUBLICATION debezium_pub FOR TABLE customers, orders, order_items;

-- ----------------------------------------------------------------------
-- Données seed
-- ----------------------------------------------------------------------

INSERT INTO customers (email, first_name, last_name, country) VALUES
    ('alice@example.com',   'Alice',   'Martin',   'FR'),
    ('bob@example.com',     'Bob',     'Dupont',   'FR'),
    ('carol@example.com',   'Carol',   'Durand',   'BE'),
    ('david@example.com',   'David',   'Bernard',  'CH'),
    ('eve@example.com',     'Eve',     'Petit',    'FR'),
    ('frank@example.com',   'Frank',   'Robert',   'DE'),
    ('grace@example.com',   'Grace',   'Richard',  'FR'),
    ('hugo@example.com',    'Hugo',    'Moreau',   'FR'),
    ('iris@example.com',    'Iris',    'Laurent',  'BE'),
    ('jade@example.com',    'Jade',    'Simon',    'FR');

INSERT INTO orders (customer_id, status, total_amount, currency) VALUES
    (1,  'paid',      149.90, 'EUR'),
    (1,  'delivered',  39.50, 'EUR'),
    (2,  'pending',   299.00, 'EUR'),
    (3,  'shipped',    79.99, 'EUR'),
    (4,  'paid',      450.00, 'EUR'),
    (5,  'cancelled',  29.90, 'EUR'),
    (6,  'delivered', 119.00, 'EUR'),
    (7,  'paid',       59.90, 'EUR'),
    (8,  'shipped',   189.50, 'EUR'),
    (9,  'pending',   349.00, 'EUR'),
    (10, 'paid',       89.00, 'EUR'),
    (2,  'paid',      210.00, 'EUR');

INSERT INTO order_items (order_id, sku, product_name, quantity, unit_price) VALUES
    (1,  'SKU-001', 'Casque audio Pro',           1, 149.90),
    (2,  'SKU-002', 'Cable USB-C 2m',             1,  39.50),
    (3,  'SKU-003', 'Clavier mecanique RGB',      1, 129.00),
    (3,  'SKU-004', 'Souris ergonomique',         2,  85.00),
    (4,  'SKU-005', 'Hub USB 7 ports',            1,  79.99),
    (5,  'SKU-006', 'Ecran 27 pouces 4K',         1, 450.00),
    (6,  'SKU-007', 'Tapis de souris XL',         1,  29.90),
    (7,  'SKU-008', 'Webcam HD 1080p',            2,  59.50),
    (8,  'SKU-009', 'Microphone USB',             1,  59.90),
    (9,  'SKU-010', 'Support ecran reglable',     1, 189.50),
    (10, 'SKU-011', 'Disque SSD externe 1To',     1, 149.00),
    (10, 'SKU-012', 'Adaptateur HDMI',            4,  50.00),
    (11, 'SKU-013', 'Lampe de bureau LED',        1,  89.00),
    (12, 'SKU-014', 'Station d''accueil USB-C',   1, 210.00);

-- ----------------------------------------------------------------------
-- Trigger updated_at
-- ----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customers_touch BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();
CREATE TRIGGER orders_touch    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at();
