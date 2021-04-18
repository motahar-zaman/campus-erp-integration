import json
import pika
import sys
import os
from executor import execute
from decouple import config
from status_history import save_status_history


def main():
    status_history_data = {}
    AMQP_USER = config('AMQP_USER')
    AMQP_PASS = config('AMQP_PASS')
    AMQP_HOST = config('AMQP_HOST')
    AMQP_PORT = config('AMQP_PORT')

    amqp_url = f'amqps://{AMQP_USER}:{AMQP_PASS}@{AMQP_HOST}:{AMQP_PORT}?connection_attempts=5&retry_delay=5'
    connection = pika.BlockingConnection(
        pika.URLParameters(amqp_url))

    channel = connection.channel()

    channel.queue_declare(queue='enrollments')

    def callback(ch, method, properties, body):
        print('task received')
        # data = json.loads(body.decode())
        # status_data = {'comment': 'received', 'data': data}
        # execute(data)
        # save_status_history(status_data)

    channel.basic_consume(
        queue='enrollments', on_message_callback=callback, auto_ack=True)

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
