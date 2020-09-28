import cloudpickle as cp
from urllib.request import urlopen
import logging
import os
import numpy as np
import argparse
from kafka import KafkaConsumer
from json import loads

def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('readtopic={}'.format(args.readtopic))
    logging.info('creating kafka consumer')

    consumer = KafkaConsumer(
        args.readtopic,
        bootstrap_servers=args.brokers,
        value_deserializer=lambda val: loads(val.decode('utf-8')))
    logging.info("finished creating kafka consumer")

    model=cp.load(urlopen(args.model))

    while True:
        for message in consumer:
            data=message.value
            logging.info(data)
            prices=data['c']
            volume=data['v']
            ticker=1#Currently hard coded in bc we are only using SPY
            log_prices=np.log(np.diff(prices))
            prediction=model.predict([[ticker,log_prices[:],volume[:]]])
            logging.info(prediction)

def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, "") != "" else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.readtopic = get_arg('KAFKA_READ_TOPIC', args.readtopic)
    args.model = get_arg('MODEL_URL', args.model)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='consume some stuff on kafka')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='kafka:9092')
    parser.add_argument(
            '--readtopic',
            help='Topic to read from, env variable KAFKA_READ_TOPIC',
            default='benign-batch-status')
    parser.add_argument(
            '--model',
            help='URL of base model to retrain, env variable MODEL_URL',
            default='https://www.dropbox.com/s/96yv0r2gqzockmw/cifar-10_ratio%3D0.5.h5?dl=1')
    

    args = parse_args(parser)
    main(args)
    logging.info('exiting')
    
# get model
# create kafka consumer
# while(1):
# get message from kafka
# parse json
# run regression
# output results?
#model = cp.load(urlopen("https://raw.githubusercontent.com/grantjensen/Backtesting2/master/myModel.cpickle"))
#model.predict([[1,0,0,0,0,0,0,0,0,0,0]])