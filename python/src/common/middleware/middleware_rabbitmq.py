import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareMessageError
import socket

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=queue_name)
        self._queue_name = queue_name
        self._user_callback = None

    def send(self, message):
        self._channel.basic_publish(exchange="", routing_key=self._queue_name, body=message)

    def close(self):
        try:
            self._channel.close()
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)

    def start_consuming(self, on_messaging_callback):
        try:
            self._user_callback = on_messaging_callback
            self._channel.basic_consume(
                queue=self._queue_name, 
                on_message_callback=self._on_messaging_callback_adapter)
            self._channel.start_consuming()
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def _on_messaging_callback_adapter(self, ch, method, properties, body):
        ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
        self._user_callback(body, ack, nack)

    
    def stop_consuming(self):
        self._channel.stop_consuming()


        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
