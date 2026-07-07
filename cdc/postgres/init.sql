-- OLTP source schema for CDC demos (Debezium requires REPLICA IDENTITY for deletes)
-- plus a dedicated database for the MLflow Tracking Server backend store.
CREATE DATABASE mlflow OWNER cdc_user;

CREATE TABLE public.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    full_name   TEXT NOT NULL,
    email       TEXT UNIQUE NOT NULL,
    segment     TEXT NOT NULL DEFAULT 'standard',
    country     TEXT NOT NULL DEFAULT 'CO',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
    order_id    BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES public.customers(customer_id),
    status      TEXT NOT NULL DEFAULT 'created',
    amount      NUMERIC(12,2) NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders    REPLICA IDENTITY FULL;

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER customers_touch BEFORE UPDATE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER orders_touch BEFORE UPDATE ON public.orders
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

INSERT INTO public.customers (full_name, email, segment, country) VALUES
    ('Alice Rivera',  'alice@example.com',  'premium',  'CO'),
    ('Bruno Castro',  'bruno@example.com',  'standard', 'MX'),
    ('Carla Mendez',  'carla@example.com',  'premium',  'AR'),
    ('Diego Torres',  'diego@example.com',  'standard', 'CL');

INSERT INTO public.orders (customer_id, status, amount, currency) VALUES
    (1, 'created',   129.90, 'USD'),
    (1, 'paid',       59.50, 'USD'),
    (2, 'created',    15.00, 'USD'),
    (3, 'shipped',   420.00, 'USD');

-- Dedicated backend store for the MLflow tracking server (local stack)
CREATE DATABASE mlflow OWNER cdc_user;
