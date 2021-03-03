import json
import pika
import sys
import os
from executor import execute
from decouple import config


def main():
    connection = pika.BlockingConnection(
        pika.URLParameters(config('AMQP_URL')))

    channel = connection.channel()

    channel.queue_declare(queue='students')

    def callback(ch, method, properties, body):
        print(' [x] Received task. Trying to execute...')
        data = json.loads(body.decode())
        execute(data)
        print(' [x] Done')

    channel.basic_consume(
        queue='students', on_message_callback=callback, auto_ack=True)

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
