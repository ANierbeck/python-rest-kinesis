#!/usr/bin/env python3
import datetime
import logging

import connexion
from connexion import NoContent
import boto3
import json

import orm

db_session = None


def get_pets(limit, animal_type=None):
    q = db_session.query(orm.Pet)
    if animal_type:
        q = q.filter(orm.Pet.animal_type == animal_type)
    return [p.dump() for p in q][:limit]


def get_pet(pet_id):
    pet = db_session.query(orm.Pet).filter(orm.Pet.id == pet_id).one_or_none()
    return pet.dump() if pet is not None else ('Not found', 404)


def put_pet(pet_id, pet):
    pet['id'] = pet_id

    pet_string = json.dumps(pet,  indent=4, sort_keys=True, default=str)

    kinesis_client.put_record(StreamName='Pets', Data=pet_string.encode(), PartitionKey='partitionkey')

    return NoContent, (200)


def delete_pet(pet_id):
    pet = db_session.query(orm.Pet).filter(orm.Pet.id == pet_id).one_or_none()
    if pet is not None:
        logging.info('Deleting pet %s..', pet_id)
        db_session.query(orm.Pet).filter(orm.Pet.id == pet_id).delete()
        db_session.commit()
        return NoContent, 204
    else:
        return NoContent, 404


kinesis_client = boto3.client('kinesis')

logging.basicConfig(level=logging.INFO)
db_session = orm.init_db('sqlite:///example.db')
app = connexion.FlaskApp(__name__)
app.add_api('swagger.yaml')

application = app.app


@application.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

def stream_init():
    logging.info('initializing the kinesis streaming ... ')
    try:
        kinesis_client.create_stream(StreamName='Pets', ShardCount=1)
    except:
        logging.info('already found a Pets streaming ... ')


if __name__ == '__main__':
    stream_init()
    app.run(
        port=8080,
        threaded=False  # in-memory database isn't shared across threads
    )

    logging.info('tear down stream ... ')
    kinesis_client.delete_stream(StreamName='Pets', EnforceConsumerDeletion=True)