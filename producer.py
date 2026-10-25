"""Publish sample order events to RabbitMQ."""

import argparse
import json
import uuid

import pika

from order_common import QUEUE_NAME


def make_order(item_id, quantity, event_id=None):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "item_id": item_id,
        "quantity": quantity,
    }


def main():
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--host", default="localhost")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.host))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    try:
        for index in range(args.count):
            order = make_order(item_id=f"item-{(index % 3) + 1}", quantity=(index % 3) + 1)
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(order),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
            print(f"[producer] published event_id={order['event_id']} order_id={order['order_id']}", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
