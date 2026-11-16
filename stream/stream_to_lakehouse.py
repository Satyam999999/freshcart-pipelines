"""
Bonus Task: Stream into the Lakehouse (Connect Part D to Part B)

Receives FreshCart-style order messages from RabbitMQ, collects them for
one minute (or configured window), writes them with write_landing() as raw
NDJSON files, loads them into Bronze with load_file(), and triggers the
transformation SQL chain to populate gold.fct_orders.

Demonstrates idempotency: running the batch twice does not double gold totals.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
import sys
import time
from uuid import uuid4

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pika
from dotenv import load_dotenv

from elt.extract import write_landing
from elt.load_bronze import load_file
from elt.run_pipeline import SQL_STAGES, _run_sql_file
from elt.snowflake_io import get_snowflake_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("stream_to_lakehouse")

load_dotenv()

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/%2F"
)
DEFAULT_QUEUE = "freshcart-orders"
DEFAULT_EXCHANGE = "freshcart-exchange"

SAMPLE_CUSTOMERS = [
    {"email": "priya.r@gmail.com", "name": "Priya R", "pincode": "600041"},
    {"email": "arjun.k@yahoo.in", "name": "Arjun K", "pincode": "560034"},
    {"email": "meera@outlook.com", "name": "Meera S", "pincode": "400050"},
    {"email": "rahul.m@gmail.com", "name": "Rahul M", "pincode": "110001"},
]


def generate_sample_order(order_num: int) -> dict[str, object]:
    """Generate a realistic FreshCart-style web order."""
    cust = random.choice(SAMPLE_CUSTOMERS)
    amount = round(random.uniform(500.0, 3500.0), 2)
    now = datetime.now(timezone.utc)
    return {
        "order_id": f"ORD-STREAM-{order_num:04d}",
        "cust_email": cust["email"],
        "cust_name": cust["name"],
        "order_date": now.strftime("%Y-%m-%d"),
        "amount": f"{amount:,.2f}",
        "currency": "INR",
        "pincode": cust["pincode"],
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def publish_sample_orders(count: int = 5, queue_name: str = DEFAULT_QUEUE) -> list[dict[str, object]]:
    """Publish sample FreshCart orders into RabbitMQ for testing."""
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as err:
        print(f"\n[ERROR] Could not connect to RabbitMQ broker at {RABBITMQ_URL}")
        print("Please ensure Docker Desktop and RabbitMQ are running:")
        print("  docker start freshcart-rabbitmq\n")
        raise SystemExit(1) from err

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    orders: list[dict[str, object]] = []
    print(f"\n[Publisher] Publishing {count} FreshCart orders to RabbitMQ queue '{queue_name}'...")

    for i in range(1, count + 1):
        order = generate_sample_order(i)
        orders.append(order)
        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(order),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type="application/json",
            ),
        )
        print(f"  -> Published: {order['order_id']} | {order['cust_email']} | {order['amount']} {order['currency']}")

    connection.close()
    print(f"[Publisher] Finished publishing {count} orders.\n")
    return orders


def collect_orders_for_duration(
    duration_seconds: int = 60,
    queue_name: str = DEFAULT_QUEUE,
    max_records: int | None = None,
) -> list[dict[str, object]]:
    """
    Connect to RabbitMQ and collect FreshCart order messages for the specified duration.
    Acks all valid consumed messages.
    """
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as err:
        print(f"\n[ERROR] Could not connect to RabbitMQ broker at {RABBITMQ_URL}")
        print("Please ensure Docker Desktop and RabbitMQ are running:")
        print("  docker start freshcart-rabbitmq\n")
        raise SystemExit(1) from err

    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    collected_records: list[dict[str, object]] = []
    start_time = time.time()
    end_time = start_time + duration_seconds

    print(f"\n[Consumer] Listening on RabbitMQ queue '{queue_name}' for {duration_seconds}s...")

    try:
        while time.time() < end_time:
            # Poll for messages with a short timeout
            method_frame, header_frame, body = channel.basic_get(
                queue=queue_name,
                auto_ack=False,
            )

            if method_frame:
                try:
                    payload = json.loads(body.decode("utf-8"))
                    collected_records.append(payload)
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    print(
                        f"  [RECEIVED] order_id={payload.get('order_id')} "
                        f"cust={payload.get('cust_email')} "
                        f"amount={payload.get('amount')} (Total: {len(collected_records)})"
                    )
                except Exception as exc:
                    log.error("Failed to parse message: %s", exc)
                    channel.basic_nack(
                        delivery_tag=method_frame.delivery_tag,
                        requeue=False,
                    )

                if max_records and len(collected_records) >= max_records:
                    print(f"[Consumer] Reached max records limit ({max_records}). Stopping early.")
                    break
            else:
                # No message in queue right now; sleep briefly to avoid busy loop
                time.sleep(0.5)

    finally:
        if connection.is_open:
            connection.close()

    elapsed = round(time.time() - start_time, 1)
    print(f"[Consumer] Collection window completed: {len(collected_records)} orders collected in {elapsed}s.")
    return collected_records


def run_transformations(connection) -> None:
    """Execute the multi-hop SQL transformation chain to reach gold.fct_orders."""
    print("\n--- Running SQL Transformation Pipeline ---")
    stages = ("clean", "dedup", "scd2", "fx", "geo", "gold")
    for stage_name in stages:
        sql_file = SQL_STAGES[stage_name]
        print(f"  -> Executing stage '{stage_name}' ({sql_file})...")
        _run_sql_file(connection, sql_file)
    print("--- Transformation Pipeline Complete ---\n")


def query_gold_orders_summary(connection) -> tuple[int, float]:
    """Query current row count and sum of AMOUNT_INR in gold.fct_orders."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(SUM(AMOUNT_INR), 0) AS total_amount_inr
            FROM FRESHCART.GOLD.FCT_ORDERS
            """
        )
        row = cursor.fetchone()
        return int(row[0]), float(row[1])


def stream_batch_to_lakehouse(
    records: list[dict[str, object]],
    run_id: str | None = None,
    run_date: date | None = None,
    connection=None,
) -> Path:
    """
    1. Writes raw records using write_landing() as compressed NDJSON.
    2. Calls load_file() into Bronze.
    3. Runs the transformation SQL stages into Gold.
    """
    if not records:
        print("[Lakehouse] No records to process.")
        return None

    run_id = run_id or f"stream_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:6]}"
    run_date = run_date or datetime.now(timezone.utc).date()

    # Step 1: Write raw landing file
    print(f"\n[Step 1] Calling write_landing(source='web', records={len(records)}, run_id='{run_id}')...")
    landing_path = write_landing(
        source="web",
        records=records,
        run_id=run_id,
        run_date=run_date,
    )
    print(f"  -> Raw landing file written to: {landing_path}")

    # Step 2: Load into Bronze with load_file()
    print(f"\n[Step 2] Calling load_file(connection, '{landing_path}', source='web')...")
    load_file(connection, landing_path, "web")

    # Step 3: Run transform SQL
    print("\n[Step 3] Running transformations to reach gold.fct_orders...")
    run_transformations(connection)

    return landing_path


def demonstrate_idempotency(records: list[dict[str, object]], connection) -> None:
    """
    Demonstrate that running the batch twice does not double gold totals.
    """
    print("\n" + "=" * 65)
    print("IDEMPOTENCY VERIFICATION: STREAMING BATCH TWICE INTO LAKEHOUSE")
    print("=" * 65)

    # Initial baseline
    count_init, sum_init = query_gold_orders_summary(connection)
    print(f"\n[Baseline] Gold FCT_ORDERS before run: Count={count_init}, Total INR={sum_init:,.2f}")

    # First Run
    run_id = f"batch_{uuid4().hex[:8]}"
    today = datetime.now(timezone.utc).date()
    print(f"\n>>> RUN #1: Ingesting and transforming batch ({len(records)} orders)...")
    landing_file = stream_batch_to_lakehouse(records, run_id=run_id, run_date=today, connection=connection)

    count_run1, sum_run1 = query_gold_orders_summary(connection)
    print(f"\n[After Run #1] Gold FCT_ORDERS: Count={count_run1}, Total INR={sum_run1:,.2f}")

    # Second Run (Exact same batch and file)
    print("\n>>> RUN #2: Ingesting and transforming the EXACT SAME batch a second time...")
    print(f"Calling load_file() again with: {landing_file}")
    load_file(connection, landing_file, "web")

    print("Re-running transformation SQL stages...")
    run_transformations(connection)

    count_run2, sum_run2 = query_gold_orders_summary(connection)
    print(f"\n[After Run #2] Gold FCT_ORDERS: Count={count_run2}, Total INR={sum_run2:,.2f}")

    # Comparison and Assertions
    print("\n" + "-" * 65)
    print(f"{'Metric':<25} | {'Run 1':<15} | {'Run 2':<15} | {'Status'}")
    print("-" * 65)
    cnt_match = "IDENTICAL (PASS)" if count_run1 == count_run2 else "FAILED"
    sum_match = "IDENTICAL (PASS)" if abs(sum_run1 - sum_run2) < 0.01 else "FAILED"
    print(f"{'Order Count':<25} | {count_run1:<15} | {count_run2:<15} | {cnt_match}")
    print(f"{'Total Amount (INR)':<25} | {sum_run1:<15,.2f} | {sum_run2:<15,.2f} | {sum_match}")
    print("-" * 65)

    assert count_run1 == count_run2, f"Count changed! Run 1: {count_run1}, Run 2: {count_run2}"
    assert abs(sum_run1 - sum_run2) < 0.01, f"Total sum changed! Run 1: {sum_run1}, Run 2: {sum_run2}"

    print("\n[VERIFIED] Running the batch twice DOES NOT double the gold totals.")
    print("Architectural guarantees:")
    print(" 1. Bronze Ledger: Ingestion ledger tracks SHA-256 fingerprint, skipping redundant raw loads.")
    print(" 2. Silver Dedup: MERGE INTO SILVER.ORDERS_DEDUP ON target.ORDER_ID = source.ORDER_ID.")
    print(" 3. Gold Mart: MERGE INTO GOLD.FCT_ORDERS ON target.ORDER_ID = source.ORDER_ID.\n")


def main():
    parser = argparse.ArgumentParser(description="Stream FreshCart orders from RabbitMQ into Snowflake Lakehouse.")
    parser.add_argument("--duration", type=int, default=60, help="Seconds to collect orders from RabbitMQ (default: 60)")
    parser.add_argument("--queue", type=str, default=DEFAULT_QUEUE, help="RabbitMQ queue name")
    parser.add_argument("--publish-sample", type=int, default=0, help="Publish N sample FreshCart orders before collecting")
    parser.add_argument("--demo-idempotency", action="store_true", help="Run the two-batch idempotency verification test")
    parser.add_argument("--max-records", type=int, default=None, help="Stop collecting once N records are gathered")
    args = parser.parse_args()

    # Mode 1: Publisher only
    if args.publish_sample > 0 and not args.demo_idempotency and "--duration" not in sys.argv:
        publish_sample_orders(count=args.publish_sample, queue_name=args.queue)
        print(f"[Done] Published {args.publish_sample} orders to queue '{args.queue}'. Now run with --duration to consume.")
        return

    # If publish_sample was combined with other steps
    if args.publish_sample > 0:
        publish_sample_orders(count=args.publish_sample, queue_name=args.queue)

    # Mode 2: Idempotency demo
    if args.demo_idempotency:
        connection = get_snowflake_connection()
        try:
            test_orders = [generate_sample_order(i) for i in range(101, 106)]
            demonstrate_idempotency(test_orders, connection)
        finally:
            connection.close()
        return

    # Mode 3: Normal consumer mode (collect from RabbitMQ for duration)
    records = collect_orders_for_duration(
        duration_seconds=args.duration,
        queue_name=args.queue,
        max_records=args.max_records,
    )

    if not records:
        print("[Lakehouse] No orders collected during this window. Exiting.")
        return

    # Open Snowflake connection only when we have records to load
    connection = get_snowflake_connection()
    try:
        stream_batch_to_lakehouse(records, connection=connection)

        # Show final summary
        count, total = query_gold_orders_summary(connection)
        print(f"\n[Final Lakehouse Status] gold.fct_orders total rows: {count}, total INR: {total:,.2f}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
