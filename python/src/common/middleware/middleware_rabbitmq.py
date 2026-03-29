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
        channel.queue_declare(queue=queue_name, durable=True)
        
        self.queue_name = queue_name
        self.must_consume = True
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
        try:
            def callback(ch, method, properties, body):
                """
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
        except KeyboardInterrupt:
            print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
        
        


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def send(self, message):
        return super().send(message)    

    def close(self):
        return super().close()
    
    def stop_consuming(self):
        return super().stop_consuming()
    
    def start_consuming(self, on_message_callback):
        return super().start_consuming(on_message_callback)
    

