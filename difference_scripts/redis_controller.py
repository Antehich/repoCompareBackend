import hashlib
import pickle

import redis

redis_client = redis.Redis()
REDIS_EXPIRE_TIME = 604800


def some_hash(data):
    return hashlib.md5(data.encode()).hexdigest()


def cache(name, value):
    return redis_client.set(f'repo:{name}', pickle.dumps(value), ex=REDIS_EXPIRE_TIME)


def load_cached(data):
    return pickle.loads(redis_client.get(f'repo:{data}'))


def is_cached(data):
    return redis_client.exists(f'repo:{data}')
