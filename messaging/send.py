


import pika
import json

credentials = pika.PlainCredentials('doug', 'smbh0992')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.10', 5672, '/', credentials))
channel = connection.channel()
data = { "type": "test", "value": 37 }
channel.queue_declare(queue='hello')
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=json.dumps(data))
print(" [x] Sent 'Hello World!'")
connection.close()
