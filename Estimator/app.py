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

    prediction=0
    while True:
        for message in consumer:
            data=message.value
            prices=data['c']
            if (len(prices)<6):
                continue
            volume=data['v']
            log_prices=np.log(np.divide(prices[1:],prices[:-1]))
            ticker=1#Currently hard coded in bc we are only using SPY
            inp=[0]*11
            inp[0]=ticker
            for i in range(1,6):
                inp[i]=volume[6-i]
            for i in range(6,11):
                inp[i]=log_prices[10-i]
            logging.info("Input: "+str(data['t'][0]+" "+str(inp))
            with open('/data/
            #Enter (data['t'][0], data['c'][0], prediction) into pvc
            prediction=model.predict([inp])
            logging.info("Output: "+str(prediction))
        #Call update_model() once per day

def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, "") != "" else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.readtopic = get_arg('KAFKA_READ_TOPIC', args.readtopic)
    args.model = get_arg('MODEL_URL', args.model)
    return args

def update_model():
    #Download data from pvc
    #If model performed at a loss, send warning message
                         
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
            default='https://raw.githubusercontent.com/grantjensen/Backtesting2/master/myModel2.cpickle')
    

    args = parse_args(parser)
    main(args)
    logging.info('exiting')
    
