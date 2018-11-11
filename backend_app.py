#!/usr/bin/env python3
import datetime
import logging

import boto3
import json
import time

import orm


logging.basicConfig(level=logging.INFO)
db_session = orm.init_db('sqlite:///example.db')


def stream_init():
    logging.info('initializing the kinesis streaming ... ')
    try:
        kinesis_client.create_stream(StreamName='Pets', ShardCount=1)
    except:
        logging.info('already found a Pets streaming ... ')


if __name__ == '__main__':
    stream_init()

    shard_id = 'shardId-000000000000'

    kinesis_client = boto3.client('kinesis')
    shard_it = kinesis_client.get_shard_iterator(StreamName='Pets', ShardId=shard_id, ShardIteratorType='LATEST')["ShardIterator"]

    while 1 == 1:
        out: dict = kinesis_client.get_records(ShardIterator=shard_it, Limit=2)
        shard_it = out["NextShardIterator"]

        data = None
        records = out['Records']
        if records:
            data = out['Records'][0]['Data'].decode()
            logging.info('found data: %s', data)

        if data is not None:
            pet = json.loads(data)

            pet_id = pet['id']

            p = db_session.query(orm.Pet).filter(orm.Pet.id == pet_id).one_or_none()

            if p is not None:
                logging.info('Updating pet %s..', pet_id)
                p.update(**pet)
            else:
                logging.info('Creating pet %s..', pet_id)
                pet['created'] = datetime.datetime.utcnow()
                db_session.add(orm.Pet(**pet))
            db_session.commit()

        time.sleep(5)


    logging.info('tear down stream ... ')
    kinesis_client.delete_stream(StreamName='Pets', EnforceConsumerDeletion=True)
