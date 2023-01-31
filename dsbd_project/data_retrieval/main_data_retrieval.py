from prometheus_api_client import PrometheusConnect
from configs import prometheus
from flask import Flask
import mysql.connector

# mysql connection
mydb = mysql.connector.connect(
    host="db",
    user="root",
    password="kofr.VBnA.462",
    database="datastorage"
)

mycursor = mydb.cursor()

app = Flask(__name__)

app.config['PREFERRED_URL_SCHEME'] = 'http'
app.config['SERVER_NAME'] = '127.0.0.1:5002'

prometheushostname = prometheus.prometheushostname

prom = PrometheusConnect(url=prometheushostname, disable_ssl=True)


@app.get('/')
def hello():
    return 'Hello from Data retrieval'


@app.get('/all_metrics')
def get_all_metrics():
    sql = """SELECT * from metadata;"""
    # print(sql)
    mycursor.execute(sql)
    all_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        # print(x)
        list_metrics.append(x)
        all_metrics[y] = list_metrics[y][1]
        y = y + 1

    return all_metrics


@app.get('/autocorrelation_stationarity_seasonality_metadata')
def get_autocorrelation_stationarity_seasonality_metadata():
    sql = """SELECT * from metadata;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        # print(x)
        list_metrics.append(x)
        autocorrelation = list_metrics[y][2]
        stationarity = list_metrics[y][3]
        seasonality = list_metrics[y][4]

        fileJson_metrics[list_metrics[y][1]] = {"autocorrelazione": autocorrelation, "stazionarietà": stationarity,
                                                "stagionalità": seasonality}
        y = y + 1
        # print(fileJson_metrics)
    return fileJson_metrics


@app.get('/max_min_avg_std_metric')
def get_max_min_avg_std_metric():
    sql = """SELECT * from metrics;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0
    for x in mycursor:
        # print(x)
        list_metrics.append(x)
        max_metric = list_metrics[y][2]
        min_metric = list_metrics[y][3]
        avg_metric = list_metrics[y][4]
        std_metric = list_metrics[y][5]
        fileJson_metrics[list_metrics[y][1]] = {"max": max_metric, "min": min_metric, "avg": avg_metric,
                                                "std": std_metric}
        y = y + 1
        # print(fileJson_metrics)
    return fileJson_metrics


@app.get('/prediction_of_metric_values')
def get_prediction_of_metric_values():
    sql = """SELECT * from prediction;"""
    # print(sql)

    mycursor.execute(sql)
    fileJson_metrics = {}
    list_metrics = []
    y = 0

    for x in mycursor:
        # print(x)
        list_metrics.append(x)
        max_metric = list_metrics[y][2]
        min_metric = list_metrics[y][3]
        avg_metric = list_metrics[y][4]
        fileJson_metrics[list_metrics[y][1]] = {"max": max_metric, "min": min_metric, "avg": avg_metric}
        y = y + 1

        # print(fileJson_metrics)

    return fileJson_metrics


if __name__ == "__main__":

    print('API data_retrieval:\n')

    print('[hello]:', app.url_for('hello'), '\n')

    print('[get_all_metrics]:', app.url_for('get_all_metrics'), '\n')

    print('[get_autocorrelation_stationarity_seasonality_metadata]:',
          app.url_for('get_autocorrelation_stationarity_seasonality_metadata'), '\n')

    print('[get_max_min_avg_std_metric]:', app.url_for('get_max_min_avg_std_metric'), '\n')

    print('[get_prediction_of_metric_values]:', app.url_for('get_prediction_of_metric_values'), '\n')

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=None)
