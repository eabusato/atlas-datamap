IF DB_ID('atlas_test') IS NULL
BEGIN
    CREATE DATABASE atlas_test;
END
GO

USE atlas_test;
GO

IF SCHEMA_ID('atlas_test') IS NULL
BEGIN
    EXEC('CREATE SCHEMA atlas_test');
END
GO

IF OBJECT_ID('atlas_test.customer_alias', 'SN') IS NOT NULL
BEGIN
    DROP SYNONYM atlas_test.customer_alias;
END
GO

IF OBJECT_ID('atlas_test.v_active_customers', 'V') IS NOT NULL
BEGIN
    DROP VIEW atlas_test.v_active_customers;
END
GO

DROP TABLE IF EXISTS atlas_test.large_events;
DROP TABLE IF EXISTS atlas_test.audit_log;
DROP TABLE IF EXISTS atlas_test.order_items;
DROP TABLE IF EXISTS atlas_test.orders;
DROP TABLE IF EXISTS atlas_test.products;
DROP TABLE IF EXISTS atlas_test.customers;
DROP TABLE IF EXISTS atlas_test.empty_table;
GO

CREATE TABLE atlas_test.customers (
    id INT IDENTITY(1,1) PRIMARY KEY,
    email NVARCHAR(255) NOT NULL UNIQUE,
    phone NVARCHAR(20) NULL,
    name NVARCHAR(100) NOT NULL,
    status NVARCHAR(20) NOT NULL DEFAULT 'active',
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Customer registry',
    @level0type = N'SCHEMA', @level0name = N'atlas_test',
    @level1type = N'TABLE',  @level1name = N'customers';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Customer email address',
    @level0type = N'SCHEMA', @level0name = N'atlas_test',
    @level1type = N'TABLE',  @level1name = N'customers',
    @level2type = N'COLUMN', @level2name = N'email';
GO

CREATE TABLE atlas_test.products (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sku NVARCHAR(50) NOT NULL UNIQUE,
    name NVARCHAR(200) NOT NULL,
    category NVARCHAR(100) NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_qty INT NOT NULL DEFAULT 0,
    is_active BIT NOT NULL DEFAULT 1,
    metadata NVARCHAR(MAX) NULL
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Product catalog',
    @level0type = N'SCHEMA', @level0name = N'atlas_test',
    @level1type = N'TABLE',  @level1name = N'products';
GO

CREATE TABLE atlas_test.orders (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status NVARCHAR(30) NOT NULL DEFAULT 'pending',
    notes NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
        REFERENCES atlas_test.customers(id)
        ON DELETE NO ACTION
        ON UPDATE CASCADE
);
GO

CREATE TABLE atlas_test.order_items (
    id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_items_order FOREIGN KEY (order_id)
        REFERENCES atlas_test.orders(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_items_product FOREIGN KEY (product_id)
        REFERENCES atlas_test.products(id)
        ON DELETE NO ACTION
        ON UPDATE CASCADE
);
GO

CREATE TABLE atlas_test.audit_log (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL,
    action NVARCHAR(50) NOT NULL,
    payload NVARCHAR(MAX) NULL,
    occurred_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE atlas_test.empty_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    placeholder NVARCHAR(50) NULL
);
GO

CREATE TABLE atlas_test.large_events (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL,
    event_type NVARCHAR(50) NOT NULL,
    payload NVARCHAR(MAX) NULL,
    occurred_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX idx_customer_status ON atlas_test.orders (customer_id, status);
CREATE INDEX idx_customer ON atlas_test.orders (customer_id);
CREATE INDEX idx_order ON atlas_test.order_items (order_id);
CREATE INDEX idx_product ON atlas_test.order_items (product_id);
CREATE INDEX idx_large_event_type ON atlas_test.large_events (event_type);
GO

CREATE VIEW atlas_test.v_active_customers AS
SELECT id, name, email, created_at
FROM atlas_test.customers
WHERE status = 'active';
GO

CREATE SYNONYM atlas_test.customer_alias FOR atlas_test.customers;
GO

INSERT INTO atlas_test.customers (email, phone, name, status) VALUES
    (N'alice@example.com', N'+55 11 99999-0001', N'Alice Silva', N'active'),
    (N'bob@example.com',   N'+55 11 99999-0002', N'Bob Santos', N'active'),
    (N'carol@example.com', NULL,                  N'Carol Oliveira', N'inactive');
GO

INSERT INTO atlas_test.products (sku, name, category, price, stock_qty, is_active, metadata) VALUES
    (N'SKU-001', N'Notebook Pro 15', N'electronics', 4999.90, 10, 1, N'{"tier":"premium"}'),
    (N'SKU-002', N'Wireless Mouse', N'peripherals', 89.90, 150, 1, N'{"wireless":true}'),
    (N'SKU-003', N'Mechanical Keyboard', N'peripherals', 349.90, 45, 1, N'{"layout":"ansi"}');
GO

INSERT INTO atlas_test.orders (customer_id, total_amount, status, notes) VALUES
    (1, 5089.80, N'completed', N'priority'),
    (1, 89.90, N'pending', NULL),
    (2, 349.90, N'shipped', N'gift wrap'),
    (3, 4999.90, N'cancelled', NULL);
GO

INSERT INTO atlas_test.order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 4999.90),
    (1, 2, 1, 89.90),
    (2, 2, 1, 89.90),
    (3, 3, 1, 349.90),
    (4, 1, 1, 4999.90);
GO

INSERT INTO atlas_test.audit_log (customer_id, action, payload) VALUES
    (1, N'login', N'{"ip":"127.0.0.1"}'),
    (2, N'logout', N'{"ip":"127.0.0.2"}');
GO

WITH numbered AS (
    SELECT TOP (12000)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
)
INSERT INTO atlas_test.large_events (customer_id, event_type, payload)
SELECT
    ((n - 1) % 3) + 1,
    CASE WHEN n % 2 = 0 THEN N'page_view' ELSE N'checkout' END,
    CONCAT(N'{"ordinal":', n, N'}')
FROM numbered;
GO
