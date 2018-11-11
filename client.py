import boto3
import time

if __name__ == '__main__':

    shard_id = 'shardId-000000000000'

    kinesis_client = boto3.client('kinesis')
    shard_it = kinesis_client.get_shard_iterator(StreamName='Pets', ShardId=shard_id, ShardIteratorType='LATEST')["ShardIterator"]

    while 1==1:
        out = kinesis_client.get_records(ShardIterator=shard_it, Limit=2)
        shard_it = out["NextShardIterator"]
        print(out)
        time.sleep(10)