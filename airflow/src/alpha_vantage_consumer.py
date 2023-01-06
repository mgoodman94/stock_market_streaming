""" STOCK CONSUMER """
import logging
import pandas as pd
from confluent_kafka.avro import AvroConsumer
from google.cloud import storage
from params import Environment as env

logging.basicConfig(level=logging.INFO)


class AlphavantageConsumer:
    """
    Class to consume messages from kafka
    """
    def __init__(self):
        self.consumer = None
        self.stock_df = None

    def configure_consumer(self):
        """
        Configure a kafka consumer
        """
        consumer_config = {"bootstrap.servers": "localhost:9092",
                        "schema.registry.url": "http://localhost:8081",
                        "group.id": "stocks.consumer.1",
                        "auto.offset.reset": "earliest"}
        consumer = AvroConsumer(consumer_config)
        consumer.subscribe(["stocks.raw"])
        self.consumer = consumer

    def consume_messages(self):
        """
        Read any new messages into dataframe
        """
        data_list = []
        while True:
            try:
                message = self.consumer.poll(1)
                if message:
                    data_list.append(message.value())
                    msg = f"Successfully pulled a record from Topic {message.topic()}, \
                        partition: {message.partition()}, \
                        offset: {message.offset()}\nmessage key: {message.key()} || message value = {message.value()}"
                    logging.info(msg)
                    self.consumer.commit()
                else:
                    break
            except Exception as e:
                msg = f"Failed to consume message due to error: {str(e)}"
                logging.error(msg)
                raise Exception(str(e))
        self.consumer.close()
        self.stock_df = pd.DataFrame.from_dict(data_list)

    def transform_data(self):
        """
        Apply transformations to latest data
        """
        self.stock_df.Open = self.stock_df.Open.round(2)
        self.stock_df.High = self.stock_df.High.round(2)
        self.stock_df.Low = self.stock_df.Low.round(2)
        self.stock_df.Close = self.stock_df.Close.round(2)

    def load_to_gcp(self):
        """
        Load data into google cloud storage
        """
        prefix = "stock_data"
        filename = f"{self.stock_df.iloc[0]['Timestamp']}.csv"
        try:
            client = storage.Client()
            bucket = client.get_bucket(env.GCP_BUCKET_NAME)
        except Exception as msg:
            logging.error("Failed to connect to GCP")
            raise Exception(str(msg))

        try:
            bucket.blob(f"{prefix}/{filename}").upload_from_string(self.stock_df.to_csv(index=False), 'text/csv')
            logging.info(f"Successfully loaded latest records to bucket: {env.GCP_BUCKET_NAME}/{prefix}/{filename}")
        except Exception as msg:
            log = f"Loading to GCP prefix {prefix}/{filename} has failed"
            logging.error(log)
            raise Exception(str(msg))


def run_consumer():
    """
    Consumer Entrypoint
    :return:
    """
    consumer = AlphavantageConsumer()
    consumer.configure_consumer()
    consumer.consume_messages()
    consumer.transform_data()
    consumer.load_to_gcp()
