"""
TPC-DS Transaction Simulator
Continuously inserts new sales/returns/inventory records into PostgreSQL
so Debezium CDC can capture them and stream to Kafka.
"""

import os
import time
import random
import logging
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "tpcds"),
    "user":     os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin123"),
}
INTERVAL_SEC = float(os.getenv("INSERT_INTERVAL_SEC", 5))
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", 5))


# ─── DB helpers ──────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_ids(conn, table: str, pk: str, limit: int = 500) -> list:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {pk} FROM {table} ORDER BY random() LIMIT %s", (limit,))
        return [row[0] for row in cur.fetchall()]


def today_sk(conn) -> int:
    """Return d_date_sk for today from date_dim."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT d_date_sk FROM date_dim WHERE d_date = %s",
            (date.today().isoformat(),)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        # Fallback: pick any recent date_sk
        cur.execute("SELECT d_date_sk FROM date_dim ORDER BY d_date_sk DESC LIMIT 1")
        return cur.fetchone()[0]


# ─── Generators ──────────────────────────────────────────────────
def insert_store_sales(conn, n: int, date_sk: int,
                       customers: list, items: list, stores: list):
    records = []
    for _ in range(n):
        quantity = random.randint(1, 10)
        list_price = round(random.uniform(5.0, 500.0), 2)
        discount   = round(random.uniform(0.0, 0.3) * list_price, 2)
        net_paid   = round((list_price - discount) * quantity, 2)
        net_profit = round(net_paid * random.uniform(0.05, 0.4), 2)

        records.append((
            date_sk,
            random.randint(1, 86399),      # ss_sold_time_sk
            random.choice(items),           # ss_item_sk
            random.choice(customers),       # ss_customer_sk
            random.choice(stores),          # ss_store_sk
            random.randint(1, 7),           # ss_promo_sk
            random.randint(100000, 999999), # ss_ticket_number
            quantity,
            list_price,
            discount,
            net_paid,
            round(net_paid * 0.1, 2),      # ss_net_paid_inc_tax
            net_profit,
        ))

    sql = """
        INSERT INTO store_sales (
            ss_sold_date_sk, ss_sold_time_sk, ss_item_sk,
            ss_customer_sk, ss_store_sk, ss_promo_sk,
            ss_ticket_number, ss_quantity, ss_list_price,
            ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Inserted %d store_sales rows", n)


def insert_web_sales(conn, n: int, date_sk: int,
                     customers: list, items: list, sites: list):
    records = []
    for _ in range(n):
        quantity   = random.randint(1, 5)
        list_price = round(random.uniform(10.0, 800.0), 2)
        discount   = round(random.uniform(0.0, 0.25) * list_price, 2)
        net_paid   = round((list_price - discount) * quantity, 2)
        net_profit = round(net_paid * random.uniform(0.05, 0.35), 2)

        records.append((
            date_sk,
            random.randint(1, 86399),
            random.choice(items),
            random.choice(customers),
            random.choice(customers),      # ws_ship_customer_sk
            random.choice(sites),          # ws_web_site_sk
            random.randint(1, 7),
            random.randint(1000000, 9999999),
            quantity,
            list_price,
            discount,
            net_paid,
            round(net_paid * 0.1, 2),
            net_profit,
        ))

    sql = """
        INSERT INTO web_sales (
            ws_sold_date_sk, ws_sold_time_sk, ws_item_sk,
            ws_bill_customer_sk, ws_ship_customer_sk, ws_web_site_sk,
            ws_promo_sk, ws_order_number, ws_quantity,
            ws_list_price, ws_coupon_amt, ws_net_paid,
            ws_net_paid_inc_tax, ws_net_profit
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Inserted %d web_sales rows", n)


def insert_catalog_sales(conn, n: int, date_sk: int,
                         customers: list, items: list, warehouses: list):
    records = []
    for _ in range(n):
        quantity   = random.randint(1, 8)
        list_price = round(random.uniform(5.0, 600.0), 2)
        discount   = round(random.uniform(0.0, 0.2) * list_price, 2)
        net_paid   = round((list_price - discount) * quantity, 2)
        net_profit = round(net_paid * random.uniform(0.05, 0.3), 2)

        records.append((
            date_sk,
            random.randint(1, 86399),
            random.choice(items),
            random.choice(customers),
            random.choice(customers),
            random.choice(warehouses),
            random.randint(1, 7),
            random.randint(1000000, 9999999),
            quantity,
            list_price,
            discount,
            net_paid,
            round(net_paid * 0.1, 2),
            net_profit,
        ))

    sql = """
        INSERT INTO catalog_sales (
            cs_sold_date_sk, cs_sold_time_sk, cs_item_sk,
            cs_bill_customer_sk, cs_ship_customer_sk, cs_warehouse_sk,
            cs_promo_sk, cs_order_number, cs_quantity,
            cs_list_price, cs_coupon_amt, cs_net_paid,
            cs_net_paid_inc_tax, cs_net_profit
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Inserted %d catalog_sales rows", n)


def update_inventory(conn, n: int, date_sk: int,
                     items: list, warehouses: list):
    """Simulate inventory quantity changes."""
    with conn.cursor() as cur:
        for _ in range(n):
            item_sk      = random.choice(items)
            warehouse_sk = random.choice(warehouses)
            new_qty      = random.randint(0, 500)
            cur.execute(
                """
                UPDATE inventory
                SET inv_quantity_on_hand = %s
                WHERE inv_item_sk = %s AND inv_warehouse_sk = %s
                  AND inv_date_sk = (
                      SELECT inv_date_sk FROM inventory
                      WHERE inv_item_sk = %s AND inv_warehouse_sk = %s
                      ORDER BY inv_date_sk DESC LIMIT 1
                  )
                """,
                (new_qty, item_sk, warehouse_sk, item_sk, warehouse_sk),
            )
    conn.commit()
    logger.info("Updated %d inventory rows", n)


def fetch_recent_tickets(conn, table: str, ticket_col: str,
                         item_col: str, limit: int = 200) -> list:
    """Fetch (ticket_number, item_sk) pairs from recent sales for return simulation."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {ticket_col}, {item_col} FROM {table} ORDER BY random() LIMIT %s",
            (limit,)
        )
        return cur.fetchall()


def insert_store_returns(conn, n: int, date_sk: int,
                         customers: list, stores: list, reasons: list,
                         recent_sales: list):
    if not recent_sales:
        return
    records = []
    for _ in range(n):
        ticket_number, item_sk = random.choice(recent_sales)
        return_qty  = random.randint(1, 3)
        return_amt  = round(random.uniform(5.0, 300.0), 2)
        net_loss    = round(return_amt * random.uniform(0.1, 0.4), 2)

        records.append((
            date_sk,
            random.randint(1, 86399),          # sr_return_time_sk
            item_sk,
            random.choice(customers),           # sr_customer_sk
            random.choice(stores),              # sr_store_sk
            random.choice(reasons),             # sr_reason_sk
            ticket_number,
            return_qty,
            return_amt,
            round(return_amt * 0.1, 2),        # sr_return_tax
            round(return_amt * 1.1, 2),        # sr_return_amt_inc_tax
            round(return_amt * 0.05, 2),       # sr_fee
            round(return_amt * 0.02, 2),       # sr_return_ship_cost
            round(return_amt * 0.7, 2),        # sr_refunded_cash
            round(return_amt * 0.1, 2),        # sr_reversed_charge
            round(return_amt * 0.1, 2),        # sr_store_credit
            net_loss,
        ))

    sql = """
        INSERT INTO store_returns (
            sr_returned_date_sk, sr_return_time_sk, sr_item_sk,
            sr_customer_sk, sr_store_sk, sr_reason_sk,
            sr_ticket_number, sr_return_quantity, sr_return_amt,
            sr_return_tax, sr_return_amt_inc_tax, sr_fee,
            sr_return_ship_cost, sr_refunded_cash,
            sr_reversed_charge, sr_store_credit, sr_net_loss
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Inserted %d store_returns rows", n)


def insert_web_returns(conn, n: int, date_sk: int,
                       customers: list, pages: list, reasons: list,
                       recent_sales: list):
    if not recent_sales:
        return
    records = []
    for _ in range(n):
        order_number, item_sk = random.choice(recent_sales)
        return_qty  = random.randint(1, 2)
        return_amt  = round(random.uniform(10.0, 500.0), 2)
        net_loss    = round(return_amt * random.uniform(0.1, 0.35), 2)

        records.append((
            date_sk,
            random.randint(1, 86399),          # wr_returned_time_sk
            item_sk,
            random.choice(customers),           # wr_refunded_customer_sk
            random.choice(customers),           # wr_returning_customer_sk
            random.choice(pages) if pages else 1,  # wr_web_page_sk
            random.choice(reasons),             # wr_reason_sk
            order_number,
            return_qty,
            return_amt,
            round(return_amt * 0.1, 2),
            round(return_amt * 1.1, 2),
            round(return_amt * 0.05, 2),
            round(return_amt * 0.02, 2),
            round(return_amt * 0.7, 2),
            round(return_amt * 0.1, 2),
            round(return_amt * 0.1, 2),        # wr_account_credit
            net_loss,
        ))

    sql = """
        INSERT INTO web_returns (
            wr_returned_date_sk, wr_returned_time_sk, wr_item_sk,
            wr_refunded_customer_sk, wr_returning_customer_sk,
            wr_web_page_sk, wr_reason_sk, wr_order_number,
            wr_return_quantity, wr_return_amt, wr_return_tax,
            wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost,
            wr_refunded_cash, wr_reversed_charge,
            wr_account_credit, wr_net_loss
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Inserted %d web_returns rows", n)


def insert_catalog_returns(conn, n: int, date_sk: int,
                           customers: list, warehouses: list, call_centers: list,
                           ship_modes: list, reasons: list, recent_sales: list):
    if not recent_sales:
        return
    records = []
    for _ in range(n):
        order_number, item_sk = random.choice(recent_sales)
        return_qty  = random.randint(1, 4)
        return_amt  = round(random.uniform(5.0, 400.0), 2)
        net_loss    = round(return_amt * random.uniform(0.1, 0.4), 2)

        records.append((
            date_sk,
            random.randint(1, 86399),
            item_sk,
            random.choice(customers),           # cr_refunded_customer_sk
            random.choice(customers),           # cr_returning_customer_sk
            random.choice(warehouses),          # cr_warehouse_sk
            random.choice(call_centers) if call_centers else 1,
            random.choice(ship_modes) if ship_modes else 1,
            random.choice(reasons),
            order_number,
            return_qty,
            return_amt,
            round(return_amt * 0.1, 2),
            round(return_amt * 1.1, 2),
            round(return_amt * 0.05, 2),
            round(return_amt * 0.02, 2),
            round(return_amt * 0.7, 2),
            round(return_amt * 0.1, 2),
            round(return_amt * 0.1, 2),        # cr_store_credit
            net_loss,
        ))

    sql = """
        INSERT INTO catalog_returns (
            cr_returned_date_sk, cr_returned_time_sk, cr_item_sk,
            cr_refunded_customer_sk, cr_returning_customer_sk,
            cr_warehouse_sk, cr_call_center_sk, cr_ship_mode_sk,
            cr_reason_sk, cr_order_number, cr_return_quantity,
            cr_return_amt, cr_return_tax, cr_return_amt_inc_tax,
            cr_fee, cr_return_ship_cost, cr_refunded_cash,
            cr_reversed_charge, cr_store_credit, cr_net_loss
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records)
    conn.commit()
    logger.info("Inserted %d catalog_returns rows", n)


# ─── Main loop ───────────────────────────────────────────────────
def main():
    logger.info("Starting TPC-DS Transaction Simulator...")

    conn = None
    while conn is None:
        try:
            conn = get_connection()
            logger.info("Connected to PostgreSQL.")
        except Exception as e:
            logger.warning("Cannot connect to DB: %s — retrying in 5s", e)
            time.sleep(5)

    # Pre-fetch dimension IDs (refresh every 1000 batches)
    refresh_every = 1000
    customers = items = stores = warehouses = sites = []
    reasons = pages = call_centers = ship_modes = []
    recent_store_sales = recent_web_sales = recent_catalog_sales = []
    iteration = 0

    while True:
        try:
            if iteration % refresh_every == 0:
                logger.info("Refreshing dimension ID pools...")
                customers    = fetch_ids(conn, "customer",    "c_customer_sk")
                items        = fetch_ids(conn, "item",         "i_item_sk")
                stores       = fetch_ids(conn, "store",        "s_store_sk")
                warehouses   = fetch_ids(conn, "warehouse",    "w_warehouse_sk")
                sites        = fetch_ids(conn, "web_site",     "web_site_sk")
                reasons      = fetch_ids(conn, "reason",       "r_reason_sk")
                pages        = fetch_ids(conn, "web_page",     "wp_web_page_sk")
                call_centers = fetch_ids(conn, "call_center",  "cc_call_center_sk")
                ship_modes   = fetch_ids(conn, "ship_mode",    "sm_ship_mode_sk")
                current_date_sk = today_sk(conn)

            # Refresh recent sales every 50 batches for return simulation
            if iteration % 50 == 0:
                recent_store_sales   = fetch_recent_tickets(conn, "store_sales",   "ss_ticket_number", "ss_item_sk")
                recent_web_sales     = fetch_recent_tickets(conn, "web_sales",     "ws_order_number",  "ws_item_sk")
                recent_catalog_sales = fetch_recent_tickets(conn, "catalog_sales", "cs_order_number",  "cs_item_sk")

            if not customers or not items:
                logger.warning("Dimension tables are empty. Waiting for data load...")
                time.sleep(10)
                continue

            roll = random.random()
            n    = random.randint(1, BATCH_SIZE)

            if roll < 0.35:
                insert_store_sales(conn, n, current_date_sk, customers, items, stores)
            elif roll < 0.55:
                insert_web_sales(conn, n, current_date_sk, customers, items, sites)
            elif roll < 0.70:
                insert_catalog_sales(conn, n, current_date_sk, customers, items, warehouses)
            elif roll < 0.78:
                insert_store_returns(conn, n, current_date_sk, customers, stores, reasons, recent_store_sales)
            elif roll < 0.84:
                insert_web_returns(conn, n, current_date_sk, customers, pages, reasons, recent_web_sales)
            elif roll < 0.90:
                insert_catalog_returns(conn, n, current_date_sk, customers, warehouses, call_centers, ship_modes, reasons, recent_catalog_sales)
            else:
                update_inventory(conn, n, current_date_sk, items, warehouses)

            iteration += 1
            time.sleep(INTERVAL_SEC)

        except psycopg2.OperationalError as e:
            logger.error("DB connection lost: %s — reconnecting...", e)
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(5)
            conn = get_connection()

        except Exception as e:
            logger.error("Unexpected error: %s", e)
            time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
