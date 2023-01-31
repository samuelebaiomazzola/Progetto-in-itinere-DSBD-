from prometheus_api_client import PrometheusConnect, MetricsList, MetricRangeDataFrame
from prometheus_api_client.utils import parse_datetime

from confluent_kafka import Producer

from datetime import timedelta

from random import *
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm
import warnings
import pandas as pd

import xlsxwriter

import json

import sys

import time

from configs import prometheus

import logging

prom = PrometheusConnect(url=prometheus.prometheushostname, disable_ssl=True)

warnings.filterwarnings('ignore')

# Disabilita i warning "statsmodels.tsa.seasonal"
warnings.filterwarnings("ignore", category=UserWarning, module="statsmodels.tsa.seasonal")

# Disabilita i warning "sm.tsa.acf"
warnings.filterwarnings("ignore", category=UserWarning, module="statsmodels.tsa.stattools")

# Disabilita i warning "statsmodels.tsa.holtwinters"
warnings.filterwarnings("ignore", category=UserWarning, module="statsmodels.tsa.holtwinters")


def monitoring_system(diff_time, diff_time_value, diff_time_predict):
    # Create a custom filter
    class CustomFilter(logging.Filter):
        def filter(self, record):
            if record.levelno == logging.INFO:
                return True
            else:
                return False

    class CustomLogger(logging.Logger):
        def __init__(self, name, data=None, level=logging.NOTSET):
            super().__init__(name, level)
            self.data = data

        def custom_log(self, level, msg, *args, **kwargs):
            if self.isEnabledFor(level):
                self._log(level, msg, args, **kwargs)

    timer_filter = CustomFilter()

    # Creazione del formato per i messaggi di log
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Create and set custom logger
    logger_meta = CustomLogger("Logger_Time_meta", diff_time)
    logger_meta.setLevel(logging.INFO)

    # Creazione di un handler per scrivere i log in un file
    file_handler = logging.FileHandler("time_metadata.log")
    file_handler.setLevel(logging.INFO)

    # aggiungere il formato creto per i messaggi al file handler
    file_handler.setFormatter(formatter)

    # Creazione del filtro
    file_handler.addFilter(timer_filter)

    # Add the file handler to the logger
    logger_meta.addHandler(file_handler)

    # Use custom log method
    logger_meta.custom_log(logging.INFO, diff_time)



    """diff_time_value"""
    # Create and set custom logger
    logger_value = CustomLogger("Logger_Time_value", diff_time_value)
    logger_value.setLevel(logging.INFO)

    # Creazione di un handler(sarebbe il message) per scrivere i log in un file
    file_handler2 = logging.FileHandler("time_value.log")
    file_handler2.setLevel(logging.INFO)

    # aggiungere il formato creto per i messaggi al file handler
    file_handler2.setFormatter(formatter)

    # Aggiunta degli handler al logger
    logger_value.addHandler(file_handler2)

    # Creazione del filtro
    file_handler2.addFilter(timer_filter)

    # Add the file handler to the logger
    logger_value.addHandler(file_handler2)

    # Use custom log method
    logger_value.custom_log(logging.INFO, diff_time_value)



    """diff_time_predict"""
    # Create and set custom logger
    logger_predict = CustomLogger("Logger_Time_predict", diff_time_predict)
    logger_predict.setLevel(logging.INFO)

    # Creazione di un handler per scrivere i log in un file
    file_handler3 = logging.FileHandler("time_predict.log")
    file_handler3.setLevel(logging.INFO)

    # aggiungere il formato creto per i messaggi al file handler
    file_handler3.setFormatter(formatter)

    # Creazione del filtro
    file_handler.addFilter(timer_filter)

    # Add the file handler to the logger
    logger_predict.addHandler(file_handler3)

    # Use custom log method
    logger_predict.custom_log(logging.INFO, diff_time_predict)


def setting_parameters(metric_name, label_config, start_time_metadata, end_time, chunk_size):
    start = time.time()
    metric_data = prom.get_metric_range_data(
        metric_name=metric_name,
        label_config=label_config,
        start_time=start_time_metadata,
        end_time=end_time,
        chunk_size=chunk_size,
    )
    end = time.time()
    return metric_data, (end - start)


def creating_file_csv(metric_name, metric_object_list):
    """scriviamo il timestamp ed il value in un file csv"""
    new_metric = metric_name.replace(':', '_')

    xlxsname = new_metric + str('.xlsx')
    csvname = new_metric + str('.csv')

    workbook = xlsxwriter.Workbook(xlxsname)
    worksheet = workbook.add_worksheet()
    row = 0
    col = 0
    format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss.ms'})

    for item in metric_object_list.metric_values.ds:
        worksheet.write(row, col, item, format)
        row += 1

    row = 0

    for item in metric_object_list.metric_values.y:
        worksheet.write(row, col + 1, item)
        row += 1

    workbook.close()

    read_file = pd.read_excel(xlxsname)
    read_file.to_csv(csvname, index=None)

    return csvname


def kakfaJsonProducer(fileJson, key):
    broker = "kafka:29092"
    topic = "promethuesdata"

    'Producer configuration'
    """See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md"""

    conf = {'bootstrap.servers': broker}

    """Create Producer instance"""
    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('\n%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('\n%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    """Read lines from stdin, produce each line to Kafka"""
    try:
        record_key = key
        record_value = json.dumps(fileJson)
        print("\nProducing record: {}\t{}".format(record_key, record_value))
        p.produce(topic, key=record_key, value=record_value, callback=delivery_callback)

    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(p))

        """Serve delivery callback queue."""

    """ 
    NOTE: Since produce() is an asynchronous API this poll() call will most likely not serve the delivery callback 
    for the last produce()d message.
    """

    p.poll(0)

    print("\n [Il producer totale]: ", p)

    """Wait until all messages have been delivered"""
    sys.stderr.write('%% Waiting for %d deliveries\n,' % len(p))
    p.flush()


def calculate_values(metric_name, timeseries, start_time):
    """Calcoliamo il valore di massimo, minimo e media"""
    max = timeseries['value'].max()
    min = timeseries['value'].min()
    avg = timeseries['value'].mean()
    std = timeseries['value'].std()
    file_json[str(metric_name + "," + str(start_time))] = {"max": max, "min": min,
                                                           "avg": avg, "std": std}

    return file_json


def stationarity(values_data):
    stationarityTest = adfuller(values_data.dropna(), regression='c', autolag='AIC')

    if stationarityTest[1] <= 0.05:
        result_stationarity = '[p-value]: ' + str(stationarityTest[1]) + ' -> ' + ' stationary series'

    else:
        result_stationarity = '[p-value]: ' + str(stationarityTest[1]) + ' -> ' + ' no stationary series'

    return result_stationarity


def seasonal(values_data):
    result_seasonal = seasonal_decompose(values_data.dropna(), model='additive', period=5)
    serializable_result = {str(k): v for k, v in result_seasonal.seasonal.to_dict().items()}

    return serializable_result


def autocorr(values_data):
    result_autocorr = []
    time_series_detrended = sm.tsa.detrend(values_data.dropna())
    autocorrelation = sm.tsa.acf(time_series_detrended).tolist()

    return autocorrelation


def predict(timeseries):
    dictPred = {}

    timeseriesresample = timeseries.resample(rule='2T').mean(numeric_only='True')

    tsmodel = ExponentialSmoothing(timeseriesresample.dropna(), trend='add', seasonal='add', seasonal_periods=5).fit()
    prediction = tsmodel.forecast(6)  # numero di valori per 10min

    dictPred = {"max": prediction.max(), "min": prediction.min(), "avg": prediction.mean()}
    return dictPred


def valuesCalc(timeseries):
    dictValues = {}
    dictValues = {"autocorrelazione": autocorr(timeseries['value']),
                  "stazionarietà": stationarity(timeseries['value']),
                  "stagionalità": seasonal(timeseries['value'])}

    return dictValues


def choose_metrics():
    metric_names_metadata = []
    metric_names_prediction = []
    not_zero_metrics = []

    metric_data = prom.get_metric_range_data(
        metric_name='',
        label_config={'job': 'ceph-metrics'}
    )

    for metric in metric_data:
        for e1, e2 in metric["values"]:
            # print("\n[lunghezza values metrics]:", len(metric_data))
            if e2 != '0' and metric["metric"]["__name__"] not in not_zero_metrics:
                not_zero_metrics.append(metric["metric"]["__name__"])
                # print("\n[values not_zero_metrics]:", e2)

    """algoritmo scelta metriche metadata"""
    for i in range(0, 15):
        random_metric = randint(0, len(not_zero_metrics))
        metric_names_metadata.append(not_zero_metrics[random_metric])

    """algoritmo scelta metriche metadata"""
    for i in range(0, 5):
        random_metric = randint(0, len(not_zero_metrics))
        metric_names_prediction.append(not_zero_metrics[random_metric])

    return metric_names_metadata, metric_names_prediction


if __name__ == "__main__":
    file_json = {}
    file_json_prediction = {}
    time_values = {}
    diff_metadata = {}
    time_data_predict = {}
    result = {}
    result_predict = {}

    metric_name, predict_metric_name = choose_metrics()

    label_config = {'job': 'ceph-metrics'}

    start_time = ["1h", "3h", "12h"]
    start_time_metadata = parse_datetime("24h")  # Da impostare a 24h / 48h

    end_time = parse_datetime("now")

    chunk_size = timedelta(minutes=1)

    print("\nParametri impostati\n")

    for name in range(0, len(metric_name)):

        """Calcoli un set di metadati con i relativi valori"""
        metric_data, diff_meta = setting_parameters(metric_name[name], label_config, start_time_metadata, end_time,
                                                    chunk_size)

        diff_metadata[metric_name[name]] = {"time metadata: ": str(diff_meta) + " s"}

        metric_df = MetricRangeDataFrame(metric_data)

        result[metric_name[name]] = valuesCalc(metric_df)

        print("\n\nRisultati valori(autocorrelazione, stazionarietà, stagionalità) di set di metadati:")
        print("\n[metric_name]:", metric_name[name])
        print("\n[autocorrelazione]:", result[metric_name[name]].get('autocorrelazione'))
        print("\n[stazionarietà]:", result[metric_name[name]].get('stazionarietà'))
        print("\n[stagionalità]:", result[metric_name[name]].get('stagionalità'))

        """calcoli il valore di max, min, avg, dev_std della metriche per 1h,3h, 12h"""
        for h in range(0, len(start_time)):
            metric_data, diff_values = setting_parameters(metric_name[name], label_config,
                                                          parse_datetime(start_time[h]), end_time,
                                                          chunk_size)

            time_values[metric_name[name] + "," + start_time[h]] = {start_time[h]: str(diff_values) + " s"}

            metric_df = MetricRangeDataFrame(metric_data)

            file_json = calculate_values(metric_name[name], metric_df, start_time[h])

        print("\n\nRisultati valori(max, min, avg, std) di set di metadati:")
        print(file_json)

    """Predica il valore di max, min, avg nei successivi 10 minuti per un set ristretto di 5 metriche"""
    for name in range(0, len(predict_metric_name)):
        predict_metric_data, diff_meta_value = setting_parameters(predict_metric_name[name], label_config,
                                                                  start_time_metadata,
                                                                  end_time, chunk_size)

        time_data_predict[predict_metric_name[name]] = {"time value predict: ": str(diff_meta_value) + " s"}

        metric_df = MetricRangeDataFrame(predict_metric_data)

        result_predict[predict_metric_name[name]] = predict(metric_df)

        print("\n\nRisultati predizione:")
        print("\n[predict_metric_name]: ", predict_metric_name[name])
        print(result_predict[predict_metric_name[name]])

    monitoring_system(diff_metadata, time_values, time_data_predict)

    kakfaJsonProducer(result, 'etl_data_pipeline#1')
    kakfaJsonProducer(file_json, 'etl_data_pipeline#2')
    kakfaJsonProducer(result_predict, 'etl_data_pipeline#3')

sys.exit()
