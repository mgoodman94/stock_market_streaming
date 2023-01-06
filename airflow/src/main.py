import argparse
from airflow.src.alpha_vantage_producer import run_producer
from airflow.src.alpha_vantage_consumer import run_consumer


def main(task):
    """
    Entrypoint for Stock streaming
    :return:
    """
    if task == "producer":
        run_producer()
    elif task == "consumer":
        run_consumer()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', help="airflow task")
    args = parser.parse_args()
    main(args.task)
