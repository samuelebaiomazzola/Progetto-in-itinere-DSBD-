import sys

from confluent_kafka import Consumer
import json
import mysql.connector

key_vector = []

c = Consumer({
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
})

c.subscribe(['promethuesdata'])

mydb = mysql.connector.connect(
    host="db",
    user="root",
    password="kofr.VBnA.462",
    database=''
)

mycursor = mydb.cursor()


def if_table_exists(cursor, db_conn, table_name):

    """controllo se il nome della tabella passato esiste nel db"""
    sql_table = "SHOW TABLES LIKE \'" + table_name + "\';"
    table = cursor.execute(sql_table)
    row_table = db_conn.get_rows(table)
    if row_table[0]:
        return True
    else:
        return False


sql_db = "SHOW databases LIKE 'datastorage'"
database = mycursor.execute(sql_db)
row_database = mydb.get_rows(database)


if row_database[0]:
    print("Database exists!")
    sql_db = "DROP database datastorage"
    database = mycursor.execute(sql_db)
    print("Database deleted! ")
    sql = "CREATE DATABASE datastorage"
    mycursor.execute(sql)
    print("Database datastorage created!")
else:
    print("Database not exists!")
    sql = "CREATE DATABASE datastorage"
    mycursor.execute(sql)
    print("Database datastorage created!")

use_DB = mycursor.execute('use datastorage')
print('\nuse datastorage')


# table metadata
if if_table_exists(mycursor, mydb, "metadata"):
    print("\nThe table metadata exists!")
else:
    print("\nThe table metadata not exists!")
    sql = "CREATE TABLE metadata (ID_metadata int NOT NULL AUTO_INCREMENT, metric_name varchar(255), " \
          "autocorrelation text, stationarity varchar(255), seasonality text, PRIMARY KEY (ID_metadata, metric_name));"
    mycursor.execute(sql)
    print("metadata table created!\n")
    for x in mycursor:
        print(x)


# table metrics
if if_table_exists(mycursor, mydb, "metrics"):
    print("\nThe table metrics exists!")
else:
    print("\nThe table metrics not exists!")
    sql = "CREATE TABLE metrics (ID_metrics int NOT NULL AUTO_INCREMENT, metric_info varchar(255), " \
          "max DOUBLE, min DOUBLE, average DOUBLE, std DOUBLE, PRIMARY KEY (ID_metrics, metric_info));"
    mycursor.execute(sql)
    print("metrics table created!\n")
    for x in mycursor:
        print(x)


# table prediction
if if_table_exists(mycursor, mydb, "prediction"):
    print("\nThe table prediction exists!")
else:
    print("\nThe table prediction not exists!")
    sql = "CREATE TABLE prediction (ID_prediction int NOT NULL AUTO_INCREMENT, metric_name varchar(255), " \
          "max DOUBLE, min DOUBLE, avg DOUBLE, PRIMARY KEY (ID_prediction, metric_name));"
    mycursor.execute(sql)
    print("prediction table created!\n")
    for x in mycursor:
        print(x)


while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("\nConsumer error: {} ".format(msg.error()))
        continue

    if str(msg.key()) == "b'etl_data_pipeline#1'":
        fileJson2 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("\nSto stampando etlDP1:\n")
        print(fileJson2)

        sql = """INSERT INTO metadata (metric_name, autocorrelation, stationarity, seasonality) VALUES (%s,%s,%s,%s);"""
        # print(sql)

        for key in fileJson2:
            param1 = key
            param2 = str(fileJson2[key]["autocorrelazione"])
            param3 = str(fileJson2[key]["stazionarietà"])
            param4 = str(fileJson2[key]["stagionalità"])
            val = (param1, param2, param3, param4)

            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.\n")
            mycursor.execute("SELECT * FROM metadata;")

            for x in mycursor:
                print(x)

    elif str(msg.key()) == "b'etl_data_pipeline#2'":
        key_vector.append(msg.key())

        fileJson = json.loads(msg.value())
        print("\nSto stampando etlDP2:\n")
        print(fileJson)

        sql = """INSERT INTO metrics (metric_info, max, min, average, std) VALUES (%s,%s,%s,%s,%s);"""
        # print(sql)

        for key in fileJson:
            param1 = key
            param2 = fileJson[key]["max"]
            param3 = fileJson[key]["min"]
            param4 = fileJson[key]["avg"]
            param5 = fileJson[key]["std"]
            val = (param1, param2, param3, param4, param5)
            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.\n")
            mycursor.execute("SELECT * FROM metrics;")
            for x in mycursor:
                print(x)

    elif str(msg.key()) == "b'etl_data_pipeline#3'":
        fileJson3 = json.loads(msg.value())
        key_vector.append(msg.key())
        print("\nSto stampando etlDP3:\n")
        print(fileJson3)

        sql = """INSERT INTO prediction (metric_name, max, min, avg) VALUES (%s,%s,%s,%s);"""
        # print(sql)

        for key in fileJson3:
            param1 = key
            param2 = fileJson3[key]["max"]
            param3 = fileJson3[key]["min"]
            param4 = fileJson3[key]["avg"]
            val = (param1, param2, param3, param4)

            print(sql, val)
            mycursor.execute(sql, val)
            mydb.commit()
            print(mycursor.rowcount, "was inserted.\n")
            mycursor.execute("SELECT * FROM prediction;")

            for x in mycursor:
                print(x)

c.close()
sys.exit()
