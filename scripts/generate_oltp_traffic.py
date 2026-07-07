"""Synthetic OLTP traffic generator to exercise the CDC pipelines.

Emits a mixed workload of INSERT / UPDATE / DELETE against Postgres so both
the Debezium stream and the batch watermark sync have realistic changes.
"""

from __future__ import annotations

import argparse
import random
import time

import psycopg2

from lakehouse.common.config import load_config

STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]
FIRST = ["Elena", "Marco", "Sofia", "Julian", "Valeria", "Andres"]
LAST = ["Gomez", "Perez", "Rojas", "Silva", "Vargas", "Nino"]


def main(iterations: int, sleep_s: float) -> None:
    pg = load_config().cdc.postgres
    conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.database, user=pg.user, password=pg.password
    )
    conn.autocommit = True
    cur = conn.cursor()

    for i in range(iterations):
        roll = random.random()
        if roll < 0.35:
            name = f"{random.choice(FIRST)} {random.choice(LAST)}"
            cur.execute(
                "INSERT INTO customers (full_name, email, segment) VALUES (%s, %s, %s)",
                (
                    name,
                    f"{name.lower().replace(' ', '.')}.{i}@example.com",
                    random.choice(["standard", "premium"]),
                ),
            )
        elif roll < 0.70:
            cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
            (customer_id,) = cur.fetchone()
            cur.execute(
                "INSERT INTO orders (customer_id, status, amount) VALUES (%s, %s, %s)",
                (customer_id, "created", round(random.uniform(5, 900), 2)),
            )
        elif roll < 0.92:
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = "
                "(SELECT order_id FROM orders ORDER BY random() LIMIT 1)",
                (random.choice(STATUSES),),
            )
        else:
            cur.execute(
                "DELETE FROM orders WHERE order_id = "
                "(SELECT order_id FROM orders WHERE status='cancelled' "
                "ORDER BY random() LIMIT 1)"
            )
        time.sleep(sleep_s)
    print(f"Done: {iterations} operations emitted.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--sleep", type=float, default=0.25)
    args = parser.parse_args()
    main(args.iterations, args.sleep)
