import pika
import sys
import os
from decouple import config
from processors import enroll_callback, refund_callback, import_callback, publish_callback, notification_callback


def main():
    AMQP_USER = config('AMQP_USER')
    AMQP_PASS = config('AMQP_PASS')
    AMQP_HOST = config('AMQP_HOST')
    AMQP_PORT = config('AMQP_PORT')
    amqp_url = f'amqps://{AMQP_USER}:{AMQP_PASS}@{AMQP_HOST}:{AMQP_PORT}?connection_attempts=5&retry_delay=5'
    exchange_campus = 'campusmq'
    exchange_dead_letter = 'dlx'

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_campus, exchange_type='topic')
    channel.exchange_declare(exchange=exchange_dead_letter, exchange_type='topic')

    queue_enroll = 'mq_enroll'
    channel.queue_declare(queue_enroll, exclusive=True)
    channel.queue_bind(exchange=exchange_campus, queue=queue_enroll, routing_key='*.enroll')
    channel.basic_consume(queue=queue_enroll, on_message_callback=enroll_callback, auto_ack=True)

    queue_import = 'mq_import'
    channel.queue_declare(queue_import, exclusive=True)
    channel.queue_bind(exchange=exchange_campus, queue=queue_import, routing_key='*.import')
    channel.basic_consume(queue=queue_import, on_message_callback=import_callback, auto_ack=True)

    queue_refund = 'mq_refund'
    channel.queue_declare(queue_refund, exclusive=True)
    channel.queue_bind(exchange=exchange_campus, queue=queue_refund, routing_key='*.refund')
    channel.basic_consume(queue=queue_refund, on_message_callback=refund_callback, auto_ack=True)

    queue_notification = 'mq_notification'
    channel.queue_declare(queue_notification, exclusive=True)
    channel.queue_bind(exchange=exchange_campus, queue=queue_notification, routing_key='notification')
    channel.basic_consume(queue=queue_notification, on_message_callback=notification_callback, auto_ack=True)

    import_queue = channel.queue_declare('', exclusive=True)
    channel.queue_bind(exchange='campusmq', queue=import_queue.method.queue, routing_key='*.publish')
    channel.basic_consume(queue=import_queue.method.queue, on_message_callback=publish_callback, auto_ack=True)

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
