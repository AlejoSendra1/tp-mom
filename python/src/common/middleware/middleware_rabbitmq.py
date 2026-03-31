import os
import sys
import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()

        channel.queue_declare(queue=queue_name) # si no existe ya, la creo
        
        self.queue_name = queue_name
        self.channel = channel
        self.connection = connection

    def send(self, message):
        self.channel.basic_publish(exchange='',
                    routing_key=self.queue_name,
                    body=message)
    
    def close(self):
        self.connection.close()
    
    def stop_consuming(self):
        self.channel.stop_consuming()
    
    def start_consuming(self, on_message_callback):

        # para poder wrappear la funcion dada por argumento a 
        # una funcion que recibe los 4 args como obliga la libreria
        def callback(ch, method, properties, body): 
            """ de la docu de pika:
            signature on_message_callback(channel, method, properties, body), where 
            - channel: pika.channel.Channel 
            - method: pika.spec.Basic.Deliver 
            - properties: pika.spec.BasicProperties 
            - body: bytes auto_ack (bool) (if set to True, automatic acknowle)
            """
            ack_func = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack_func = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body,ack_func, nack_func)
    
        self.channel.basic_consume(queue=self.queue_name,
                        on_message_callback=callback)
        self.channel.start_consuming()
    
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_name,
            exchange_type='direct') # si no existe, lo creo
        
        self.exchange_name = exchange_name
        self.channel = channel
        self.connection = connection
        self.routing_keys = routing_keys      

    def send(self, message):
        for routing_key in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name,
                      routing_key=routing_key,
                      body=message)

    def close(self):
        self.connection.close()
    
    def stop_consuming(self):
        self.channel.stop_consuming()
    
    def start_consuming(self, on_message_callback):

        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        for routing_key in self.routing_keys:    
            self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name,  routing_key=routing_key) # la bindeo al exchange

        def callback(ch, method, properties, body): 
            """
            signature on_message_callback(channel, method, properties, body), where 
            - channel: pika.channel.Channel 
            - method: pika.spec.Basic.Deliver 
            - properties: pika.spec.BasicProperties 
            - body: bytes auto_ack (bool) (if set to True, automatic acknowle)
            """
            on_message_callback(body, 
                                lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                                lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
                                )

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback)

        self.channel.start_consuming()
        
    
