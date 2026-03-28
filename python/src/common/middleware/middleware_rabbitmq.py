import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)

    def send(self, message):
        return super().send(message)
    
    def close(self):
        return super().close()
    
    def stop_consuming(self):
        return super().stop_consuming()
    
    def start_consuming(self, on_message_callback):
        return super().start_consuming(on_message_callback)
        

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