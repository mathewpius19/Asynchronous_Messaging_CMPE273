"""Competing order worker with manual ACK and idempotent processing."""

import argparse
import os
import time

import pika

from order_common import (
    QUEUE_NAME,
    already_processed,
    claim_failure,
    decode_message,
    mark_processed,
)


def main():
    parser = argparse.ArgumentParser(description="Consume and process order messages")
    parser.add_argument("--name", required=True, help="worker name shown in logs")
    parser.add_argument("--host", default="localhost")
    parser.add_argument(
        "--fail-event-id",
        help="simulate a crash after processing but before ACK for this event, once globally",
    )
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.host))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    def handle_message(channel, method, properties, body):
        try:
            order = decode_message(body)
        except (ValueError, TypeError) as error:
            print(f"[{args.name}] invalid message; ACKing to avoid poison-message loops: {error}", flush=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        event_id = order["event_id"]
        redelivered = " redelivered=true" if method.redelivered else ""
        print(f"[{args.name}] received event_id={event_id}{redelivered}", flush=True)

        if already_processed(event_id):
            print(f"[{args.name}] duplicate detected event_id={event_id}; skipping business action", flush=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{args.name}] ACK sent event_id={event_id}", flush=True)
            return

        print(
            f"[{args.name}] processing order_id={order['order_id']} "
            f"item_id={order['item_id']} quantity={order['quantity']}",
            flush=True,
        )
        time.sleep(0.2)
        mark_processed(event_id)

        if args.fail_event_id == event_id and claim_failure(event_id):
            print(f"[{args.name}] SIMULATED CRASH before ACK event_id={event_id}", flush=True)
            os._exit(1)

        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{args.name}] ACK sent event_id={event_id}", flush=True)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message, auto_ack=False)
    print(f"[{args.name}] waiting on queue={QUEUE_NAME}; manual ACK enabled", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
