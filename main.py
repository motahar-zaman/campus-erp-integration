import pika
import sys
import os
from decouple import config
from processors import enroll_callback, refund_callback


def main():
    AMQP_USER = config('AMQP_USER')
    AMQP_PASS = config('AMQP_PASS')
    AMQP_HOST = config('AMQP_HOST')
    AMQP_PORT = config('AMQP_PORT')

    amqp_url = f'amqps://{AMQP_USER}:{AMQP_PASS}@{AMQP_HOST}:{AMQP_PORT}?connection_attempts=5&retry_delay=5'

    connection = pika.BlockingConnection(
        pika.URLParameters(amqp_url))

    channel = connection.channel()

    channel.exchange_declare(exchange='campusmq', exchange_type='topic')

    retund_queue = channel.queue_declare('', exclusive=True)
    channel.queue_bind(exchange='campusmq', queue=retund_queue.method.queue, routing_key='*.refund')
    channel.basic_consume(queue=retund_queue.method.queue, on_message_callback=refund_callback, auto_ack=True)

    enroll_queue = channel.queue_declare('', exclusive=True)
    channel.queue_bind(exchange='campusmq', queue=enroll_queue.method.queue, routing_key='*.enroll')
    channel.basic_consume(queue=enroll_queue.method.queue, on_message_callback=enroll_callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
