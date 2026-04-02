USE atlas_test;

DROP VIEW IF EXISTS v_active_customers;
DROP TABLE IF EXISTS large_events;
DROP TABLE IF EXISTS empty_table;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL UNIQUE COMMENT 'Customer email address',
    phone VARCHAR(20) DEFAULT NULL COMMENT 'Phone number',
    name VARCHAR(100) NOT NULL,
    status ENUM('active', 'inactive', 'suspended') NOT NULL DEFAULT 'active',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_status (status),
    INDEX idx_created (created_at)
) ENGINE=InnoDB COMMENT='Customer registry';

CREATE TABLE products (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    sku VARCHAR(50) NOT NULL UNIQUE COMMENT 'Product SKU',
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100) DEFAULT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_qty INT NOT NULL DEFAULT 0,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    metadata JSON DEFAULT NULL,
    PRIMARY KEY (id),
    INDEX idx_category (category),
    INDEX idx_active (is_active)
) ENGINE=InnoDB COMMENT='Product catalog';

CREATE TABLE orders (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id INT UNSIGNED NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    notes TEXT DEFAULT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_customer (customer_id),
    INDEX idx_customer_status (customer_id, status),
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB COMMENT='Orders';

CREATE TABLE order_items (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id INT UNSIGNED NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    quantity INT UNSIGNED NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_order (order_id),
    INDEX idx_product (product_id),
    CONSTRAINT fk_items_order
        FOREIGN KEY (order_id) REFERENCES orders(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_items_product
        FOREIGN KEY (product_id) REFERENCES products(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE audit_log (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id INT UNSIGNED DEFAULT NULL,
    action VARCHAR(50) NOT NULL,
    payload JSON DEFAULT NULL,
    occurred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_customer_action (customer_id, action)
) ENGINE=InnoDB COMMENT='Audit trail';

CREATE TABLE empty_table (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    placeholder VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE large_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id INT UNSIGNED DEFAULT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSON DEFAULT NULL,
    occurred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_large_event_type (event_type)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW v_active_customers AS
SELECT id, name, email, created_at
FROM customers
WHERE status = 'active';

INSERT INTO customers (email, phone, name, status) VALUES
    ('alice@example.com', '+55 11 99999-0001', 'Alice Silva', 'active'),
    ('bob@example.com', '+55 11 99999-0002', 'Bob Santos', 'active'),
    ('carol@example.com', NULL, 'Carol Oliveira', 'inactive');

INSERT INTO products (sku, name, category, price, stock_qty, metadata) VALUES
    ('SKU-001', 'Notebook Pro 15', 'electronics', 4999.90, 10, JSON_OBJECT('tier', 'premium')),
    ('SKU-002', 'Wireless Mouse', 'peripherals', 89.90, 150, JSON_OBJECT('wireless', TRUE)),
    ('SKU-003', 'Mechanical Keyboard', 'peripherals', 349.90, 45, JSON_OBJECT('layout', 'ansi'));

INSERT INTO orders (customer_id, total_amount, status, notes) VALUES
    (1, 5089.80, 'completed', 'priority'),
    (1, 89.90, 'pending', NULL),
    (2, 349.90, 'shipped', 'gift wrap'),
    (3, 4999.90, 'cancelled', NULL);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 4999.90),
    (1, 2, 1, 89.90),
    (2, 2, 1, 89.90),
    (3, 3, 1, 349.90),
    (4, 1, 1, 4999.90);

INSERT INTO audit_log (customer_id, action, payload) VALUES
    (1, 'login', JSON_OBJECT('ip', '127.0.0.1')),
    (2, 'logout', JSON_OBJECT('ip', '127.0.0.2'));

INSERT INTO large_events (customer_id, event_type, payload)
SELECT
    MOD(seq.seq_value, 3) + 1,
    CASE WHEN MOD(seq.seq_value, 2) = 0 THEN 'page_view' ELSE 'checkout' END,
    JSON_OBJECT('ordinal', seq.seq_value)
FROM (
    SELECT
        ones.n
        + tens.n * 10
        + hundreds.n * 100
        + thousands.n * 1000
        + 1 AS seq_value
    FROM
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
    CROSS JOIN
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
    CROSS JOIN
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS hundreds
    CROSS JOIN
        (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
         UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS thousands
) AS seq
WHERE seq.seq_value <= 12000;

ANALYZE TABLE customers, products, orders, order_items, audit_log, large_events;
