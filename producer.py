import pika
# import sys
import json

def amqp_connection(data,con_host='rabbitmq',exchange_name='bulk_file_ingestion',routing_key='DEFAULT_ROUTING_KEY',queue_name='sensor_data'):
   connection = pika.BlockingConnection(pika.ConnectionParameters(host=con_host))
   channel = connection.channel()
   channel.exchange_declare(exchange=exchange_name, exchange_type='topic')
   channel.queue_declare(queue=queue_name, durable=True)
   channel.basic_publish(exchange='bulk_file_ingestion',
                        routing_key=routing_key,
                        body=json.dumps(data),
                        properties=pika.BasicProperties(
                           delivery_mode = 2, # make message persistent
                        ))
   # print(" [x] Sent %r:%r" % (routing_key, message))
   connection.close()


# amqp_connection(message)