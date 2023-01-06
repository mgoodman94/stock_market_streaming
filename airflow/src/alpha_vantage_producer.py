""" STOCK DATA PRODUCER """
import logging
import requests
from time import sleep
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from src.params import Environment as env

logging.basicConfig(level=logging.INFO)


class AlphavantageProducer:
    """
    Class to produce messages from the Alpha Vantage API to Kafka
    """
    def __init__(self):
        self.producer = None
        self.raw_stock_data = []
        self.company_symbols = ["TSLA", "AMZN", "GOOGL", "AAPL", "NFLX"]

    def load_avro_schema(self):
        """
        Load key and value avsc files
        :return: key and value schemas
        """
        key_schema = avro.load("avro_schema/stock_key.avsc")
        value_schema = avro.load("avro_schema/stock_value.avsc")
        return key_schema, value_schema

    def configure_producer(self):
        """
        Configures the producer to push records to a broker
        """
        key_schema, value_schema = self.load_avro_schema()
        producer_config = {
            "bootstrap.servers": "localhost:9092",  # broker host
            "schema.registry.url": "http://localhost:8081",  # schema registry host
            "acks": "1"
        }
        producer = AvroProducer(
            config=producer_config,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )
        self.producer = producer

    def get_api_request(self, url):
        """
        Retry backoff loop to handle API request failures

        :param url: endpoint url
        :return: request object
        """
        error = None
        for i in range(env.RETRY_COUNT):
            try:
                request = requests.get(url=url)
                if request.status_code == 200:
                    return request
                else:
                    error = f"Unable to get a status code of 200. Received a status code of {request.status_code}"
                    sleep(2)
                    continue
            except Exception as e:
                logging.warning(f"Request to {url} failed on attempt {i + 1} of {env.RETRY_COUNT}. Retrying now.")
                error = str(e)
                sleep(2)
                continue
        else:
            logging.error(f"API Extract failed to reach the endpoint after {env.RETRY_COUNT} retries. Error: {error}")
            raise Exception(error)

    def extract_stock_data(self):
        """
        Extract raw real-time stock data from API
        """
        for symbol in self.company_symbols:
            url = f"{env.API_URL}&apikey={env.API_KEY}&symbol={symbol}"
            request = self.get_api_request(url)  # get request object
            data = request.json()
            last_refreshed = data["Meta Data"]["3. Last Refreshed"]  # use to get most recent data only
            record = data["Time Series (5min)"][last_refreshed]
            record["CompanySymbol"] = symbol
            record["Timestamp"] = last_refreshed
            self.raw_stock_data.append(record)

    def push_messages(self):
        """
        Push records to Kafka topic
        """
        for record in self.raw_stock_data:
            key = {"CompanySymbol": record["CompanySymbol"]}
            value = {
                "CompanySymbol": record["CompanySymbol"],
                "Timestamp": record["Timestamp"],
                "Open": float(record["1. open"]),
                "High": float(record["2. high"]),
                "Low": float(record["3. low"]),
                "Close": float(record["4. close"]),
                "Volume": int(record["5. volume"])
            }
            try:
                self.producer.produce(
                    topic="stocks.raw",
                    key=key,
                    value=value
                )
                logging.info(f"Successfully produced record: {value}")
            except Exception as error:
                logging.error(f"Error while trying to produce record {value}: {str(error)}")
            self.producer.flush()


def run_producer():
    """
    Producer Entrypoint
    :return:
    """
    producer = AlphavantageProducer()
    producer.configure_producer()  # configure the Producer
    producer.extract_stock_data()  # extract raw stock data
    producer.push_messages()  # push records to kafka topic
