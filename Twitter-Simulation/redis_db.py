from redis_utils import RedisUtils as rdu
from twitter_objects import Tweet, User
import numpy as np
from functools import lru_cache
import redis

class RedisTwitterDB:
    def __init__(self, db_num, hashes):
        r = rdu(db_num)
        for hash in hashes:
            r.


