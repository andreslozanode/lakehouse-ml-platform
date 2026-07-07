-- Scale-out path: Flink SQL job consuming the SAME Debezium topics natively
-- (format = 'debezium-json' understands the full envelope, including deletes)
-- and maintaining an upsert-materialized Iceberg table.

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://lakehouse-ml-prod/iceberg'
);

CREATE TABLE kafka_orders_cdc (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    currency STRING,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'oltp.public.orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-orders-iceberg',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.silver.orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12, 2),
    currency STRING,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

INSERT INTO iceberg_catalog.silver.orders SELECT * FROM kafka_orders_cdc;
