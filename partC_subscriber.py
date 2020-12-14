# IoT Final Exam - Part C
# Subscriber Code

import paho.mqtt.client as mqtt
import mysql.connector


temp1 = 0
temp2 = 0
ldr = 0
heater = ""
humidity1 = 0
humidity2 = 0
pump = ""

MQTT_SERVER ="localhost"

MQTT_PATH1 ="IoTClass/devices/temp1"
MQTT_PATH2 ="IoTClass/devices/temp2"
MQTT_PATH3 ="IoTClass/devices/ldr"
MQTT_PATH4 ="IoTClass/devices/heater"
MQTT_PATH5 ="IoTClass/devices/humidity1"
MQTT_PATH6 ="IoTClass/devices/humidity2"
MQTT_PATH7 ="IoTClass/devices/pump"


def on_message(client, userdata, message):
    data = message.payload.decode("utf-8")
    topic = message.topic
    if topic == MQTT_PATH1:
        global temp1
        temp1 = data
    elif topic == MQTT_PATH2:
        global temp2
        temp2 = data
    elif topic == MQTT_PATH3:
        global ldr
        ldr = data
    elif topic == MQTT_PATH4:
        global heater
        heater = data
    elif topic == MQTT_PATH5:
        global humidity1
        humidity1 = data
    elif topic == MQTT_PATH6:
        global humidity2
        humidity2 = data
    elif topic == MQTT_PATH7:
        global pump
        pump = data
        sqlInsert()


def sqlInsert():
    print("Values = ", str(temp1), str(temp2), str(ldr), heater, str(humidity1), str(humidity2), pump)
    try:
        connection = mysql.connector.connect(host='localhost', database='finalexam', user='root', password='')
        cursor = connection.cursor()
        query = """INSERT INTO partc_table (TEMP1, TEMP2, LDR, HEATER, HUMIDITY1, HUMIDITY2, PUMP) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        input_tuple = (temp1, temp2, ldr, heater, humidity1, humidity2, pump)
        cursor.execute(query, input_tuple)
        connection.commit()
        print("Record inserted successfully into partc_table")
    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def data_subscribe():
    client = mqtt.Client("local-sub-client")
    client.on_message = on_message
    print("connecting to broker")
    client.connect(MQTT_SERVER)
    print("Subscribing to topic")
    client.subscribe([(MQTT_PATH1,1),(MQTT_PATH2,1),(MQTT_PATH3,1),
                      (MQTT_PATH4,1),(MQTT_PATH5,1),(MQTT_PATH6,1),
                      (MQTT_PATH7, 1)])
    client.loop_forever()


# Main
data_subscribe()
