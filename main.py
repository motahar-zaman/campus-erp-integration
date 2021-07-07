import pika
import sys
import os
from decouple import config
from processors import mindedge_callback, hubspot_callback, product_callback, avatax_callback, requestlog_callback, send_enrollment_cancel_email_callback, send_cart_data_callback, send_tax_refund_data_callback


def main():
    AMQP_USER = config('AMQP_USER')
    AMQP_PASS = config('AMQP_PASS')
    AMQP_HOST = config('AMQP_HOST')
    AMQP_PORT = config('AMQP_PORT')

    amqp_url = f'amqps://{AMQP_USER}:{AMQP_PASS}@{AMQP_HOST}:{AMQP_PORT}?connection_attempts=5&retry_delay=5'
    connection = pika.BlockingConnection(
        pika.URLParameters(amqp_url))

    channel = connection.channel()

    channel.queue_declare(queue='enrollments')
    channel.queue_declare(queue='hubspot')
    channel.queue_declare(queue='product')
    channel.queue_declare(queue='avatax')
    channel.queue_declare(queue='cancel_enrollment')
    channel.queue_declare(queue='cancel_crm')
    channel.queue_declare(queue='cancel_tax')

    channel.basic_consume(
        queue='enrollments', on_message_callback=mindedge_callback, auto_ack=True)

    channel.basic_consume(
        queue='hubspot', on_message_callback=hubspot_callback, auto_ack=True)

    channel.basic_consume(
        queue='product', on_message_callback=product_callback, auto_ack=True)

    channel.basic_consume(
        queue='avatax', on_message_callback=avatax_callback, auto_ack=True)

    channel.basic_consume(
        queue='requestlog', on_message_callback=requestlog_callback, auto_ack=True)

    channel.basic_consume(
        queue='cancel_enrollment', on_message_callback=send_enrollment_cancel_email_callback, auto_ack=True)

    channel.basic_consume(
        queue='cancel_crm', on_message_callback=send_cart_data_callback, auto_ack=True)

    channel.basic_consume(
        queue='cancel_tax', on_message_callback=send_tax_refund_data_callback, auto_ack=True)

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
