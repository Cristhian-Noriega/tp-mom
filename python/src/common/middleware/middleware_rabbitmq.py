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
        # el routing keys que recibo, son los "filtros de interes"
        # el cliente quiere por ej conectarse al exchange de notificaciones, pero solo quiere
        # recibir los mensajes cuya routing key este dentro de esa lista 
        # el exchange entonces hace:
        # conectar y abrir el canal
        # declarar el Exchange
        # declarar una cola exclusiva y temporal para este consumidor, ya que la cola necesita saber a que exchange vincularse
        # por cada elemento en routing keys, crea un binding para esa cola y el exchange (direct)
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self._channel = self._connection.channel()
        self._exchange_name = exchange_name
        self._exchange = self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        # queue_result = self._channel.queue_declare(queue='',exclusive=True)
        # self._queue_name = queue_result.method.queue
        # for key in routing_keys:
        #     self._channel.queue_bind(queue=self._queue_name, exchange=exchange_name, routing_key=key)
        self._routing_keys = routing_keys
        self._queue_name = None

    
    def send(self, message):
        if not self._routing_keys:
            raise MessageMiddlewareMessageError("No routing keys provided")
        # como productor, quiero mandar a UNA sola ruta  (1-1)
        # luego los consumidores pueden consumir MAS de una ruta (1-N), pero no es el caso de send porque solo sirve para el consumer
        self._channel.basic_publish(exchange=self._exchange_name, routing_key=self._routing_keys[0], body=message)


    def close(self):
        try:
            self._channel.close()
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)

    def start_consuming(self, on_messaging_callback):
        self._init_queue()
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

    def _init_queue(self):
        if self._queue_name: 
            return

        queue_result = self._channel.queue_declare(queue='',exclusive=True)
        self._queue_name = queue_result.method.queue
        for key in self._routing_keys:
            self._channel.queue_bind(queue=self._queue_name, exchange=self._exchange_name, routing_key=key)