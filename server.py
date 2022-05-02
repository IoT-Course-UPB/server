import pika
from time import sleep
from threading import Thread
import psycopg2
from prometheus_client import Gauge, start_http_server
import json
import os
from flask import Flask

gauges = {}


def connect_to_rmq():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='device_queue', durable=True)
    channel.queue_declare(queue='data_queue', durable=True)
    return channel


def try_to_connect_to_rmq():
    try:
        channel = connect_to_rmq()
        return channel
    except:
        sleep(1)
        channel = try_to_connect_to_rmq()
        return channel


def device_queue_callback(ch, method, properties, body):
    cmd = body.decode()
    print("Received device info: %s" % cmd)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    json_object = json.loads(cmd)

    conn = psycopg2.connect(
        host="postgres",
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'])
    cursor = conn.cursor()

    # see if there is a device with this ID
    query = 'SELECT id FROM devices WHERE id = %s'
    cursor.execute(query, (json_object["name"], ))

    id = cursor.fetchall()

    name = json_object["name"]
    actuator_name = ""
    actuator_active = ""
    actuator_unit = ""
    sensor_name = ""
    sensor_active = ""
    sensor_unit = ""

    if "actuator" in json_object:
        actuator_name = json_object["actuator"]["name"]
        actuator_active = str(json_object["actuator"]["active"])
        actuator_unit = json_object["actuator"]["unit"]

    if "sensor" in json_object:
        sensor_name = json_object["sensor"]["name"]
        sensor_active = str(json_object["sensor"]["active"])
        sensor_unit = json_object["sensor"]["unit"]
        if sensor_active == "False":
            if name not in gauges:
                gauges[name] = Gauge(name, 'Values from sensor ' + name)
            gauges[name].set(0)

    if (len(id) == 0):
        # if there is none, add it
        cursor.execute('INSERT INTO devices (id, actuator_id, actuator_status, actuator_unit, sensor_id, sensor_status, sensor_unit)'
                       'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                       (name, actuator_name, actuator_active, actuator_unit, sensor_name, sensor_active, sensor_unit))
        gauges[name] = Gauge(name, 'Values from sensor ' + name)
    else:
        # if there is, update it
        cursor.execute('UPDATE devices SET actuator_id=%s, actuator_status=%s, actuator_unit=%s, sensor_id=%s, sensor_status=%s, sensor_unit=%s WHERE id = %s',
                       (actuator_name, actuator_active, actuator_unit, sensor_name, sensor_active, sensor_unit, name))
    conn.commit()
    cursor.close()
    conn.close()


def data_queue_callback(ch, method, properties, body):
    cmd = body.decode()
    print("Received data: %s" % cmd)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    json_object = json.loads(cmd)
    name = json_object["name"][0:-2]

    if name not in gauges:
        gauges[name] = Gauge(name, 'Values from sensor ' + name)

    gauges[name].set(float(json_object["measurement"]))


def subscriber_thread_function(channel):
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='device_queue',
                          on_message_callback=device_queue_callback)
    channel.basic_consume(queue='data_queue',
                          on_message_callback=data_queue_callback)
    channel.start_consuming()


app = Flask(__name__)

print("Server starting...")

sleep(15)

channel = connect_to_rmq()

print("Server started")

subscriber_thread = Thread(target=subscriber_thread_function, args=(channel,))
subscriber_thread.start()


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


if __name__ == "__main__":
    start_http_server(8000)
    app.run(host="0.0.0.0", port=6000)
