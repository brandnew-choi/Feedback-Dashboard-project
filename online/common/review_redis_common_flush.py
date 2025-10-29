import redis

def init_redis(host, port, db):
    client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)
    return client

def flush_db(db):

    client = init_redis(host='localhost', port=6379, db=db)
    client.flushdb()
