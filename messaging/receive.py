
import pika
import sys
import os
import json

def main():
  credentials = pika.PlainCredentials('doug', 'smbh0992')
  connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.10', 5672, '/', credentials))
  channel = connection.channel()
  channel.queue_declare(queue='hello')   

  def callback(ch, method, properties, body):
      print(json.loads(body))

  channel.basic_consume(queue='hello',
                        auto_ack=True,
                        on_message_callback=callback)

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