from redis_utils import RedisUtils
from twitter_objects import Tweet, User
import numpy as np
from functools import lru_cache
import redis
from collections import defaultdict
import time
import random
import os
import multiprocessing as mp
from multiprocessing.pool import Pool
from multiprocess.process import AuthenticationString


def mapstar(args):
    return list(map(*args))


class RedisTwitterApi:
    def __init__(self, port, host="localhost"):
        """

        :param user: string
            user in the sql database being connected to
        :param password: string
            password for the user in the sql database being connected to
        :param database: string
            name of the sql database being connected to
        :param host: string
            default "localhost"
            database server
        """
        self.dbu = RedisUtils(port=port, host=host)
        self.next_tweet_id = 1
        self.current_timeline_num = 1
        self.old_timeline_num = 0
        self.avail_procs = [mp.Process() for cpu in range(mp.cpu_count() - 1)]
        self.updated_timelines = False
        self.updated_cache = False

    def add_follows_followers(self, filename):
        r = self.dbu.con
        r.flushdb()
        with open(filename, 'r') as infile:
            next(infile)
            while True:
                line = infile.readline().strip().split(',')
                if len(line) < 2:
                    break
                r.sadd('follows_' + line[0], line[1])
                r.sadd('followers_' + line[1], line[0])
                r.sadd('users', line[0])
                r.sadd('users', line[1])

    @lru_cache(maxsize=10000)
    def get_followers(self, user_id):
        r = self.dbu.con
        return r.smembers('followers_' + user_id)

    def update_timelines(self):
        r = self.dbu.con
        # os.system('redis-cli --scan --pattern timeline_* | xargs redis-cli del')
        count = self.next_tweet_id - 1
        timeline_lens = defaultdict(lambda: 0)
        while any(val != 10 for val in timeline_lens.values()) or not timeline_lens:
            tweet = r.hgetall('tweets:' + str(count))
            user_id = tweet['user_id']
            followers = self.get_followers(user_id)
            if followers:
                for follower in followers:
                    if timeline_lens[follower] < 10:
                        timeline_lens[follower] += 1
                        r.sadd('timeline_' + str(follower), user_id + '|' + tweet['tweet_text'])
                        '''
                        r.zadd('timeline_' + str(self.current_timeline_num) + '_' + str(follower),
                               {user_id + '|' + tweet['tweet_text']: timeline_lens[follower]})
                        #timeline_lens[follower] += 1

                        r.zadd('timeline_' + str(follower),
                               {user_id + '|' + tweet['tweet_text']: timeline_lens[follower]})
                        '''
                        # r.rpush('timeline_' + str(follower), user_id + '|' + tweet['tweet_text'])
            count -= 1
            if count == 0:
                break
        # print(r.smembers('timeline_'+ str(self.current_timeline_num) + '_2942'))
        self.current_timeline_num += 1

    def drop_old_timelines(self):
        for i in range(self.old_timeline_num, self.current_timeline_num - 1):
            os.system('redis-cli --scan --pattern timeline_' + str(i) + '_* | xargs redis-cli del')
        self.old_timeline_num = self.current_timeline_num

    def read_load_tweets(self, filename, batch=500000):

        """

        :param filename: string
            the name of a file containing tweets to be posted
        :param batch: int
            the number of tweets to insert at once
        :return: None
        """
        r = self.dbu.con
        with open(filename, 'r') as infile:
            next(infile)
            # timeline_nums = []
            while True:
                for i in range(batch):
                    line = infile.readline().strip().split(',')

                    # if the line is empty, don't insert and leave while loop
                    if len(line) < 2:
                        break
                    # put tweet in all followers timelines
                    r.hset('tweets:' + str(self.next_tweet_id),
                           mapping={'user_id': line[0], 'tweet_text': line[1], 'tweet_ts': time.time()})
                    # followers = self.get_followers(line[0])
                    # if followers:
                    #    for follower in followers:
                    # r.zadd('timeline_' + str(self.current_timeline_num) + '_' + str(follower),
                    #       {line[0] + '|' + line[1]: self.next_tweet_id})
                    # r.zadd('timeline_' + str(follower),
                    # {line[0] + '|' + line[1]: self.next_tweet_id})
                    self.next_tweet_id += 1
                # timeline_nums.append(self.current_timeline_num)
                # proc = first incactive process
                # self.update_timelines()
                if len(line) < 2:
                    break
        self.update_timelines()
        # self.drop_old_timelines()

        '''
        count = self.next_tweet_id - 1
        timeline_lens = defaultdict(lambda: 0)
        while count > 0:
            tweet = r.hgetall('tweets:' + str(count))
            user_id = tweet['user_id']
            followers = self.get_followers(user_id)
            if followers:
                for follower in followers:
                    if timeline_lens[follower] < 10:
                        r.sadd('timeline_' + str(follower), user_id + '|' + tweet['tweet_text'])
                        timeline_lens[follower] += 1
            count -= 1
        #end = time.time()
        #print('part 2', round((end - start), 2))
        '''
        # self.update_timelines()

    @lru_cache(maxsize=1000)
    def load_timeline(self, input_user):
        """

        :param input_user: int
            user id to load the timeline for
        :return: list
            contains tweet objects of the 10 most recent tweets by users followed by the input user
        """
        r = self.dbu.con
        timeline = r.smembers('timeline_' + str(input_user))
        # timeline = r.lrange('timeline_' + str(input_user), -0, -1)
        # timeline = r.zrange('timeline_' + str(input_user), -10, -1)
        # timeline = r.zrange('timeline_' + str(self.current_timeline_num - 1) + '_' + str(input_user), -10, -1)
        if timeline:
            return [Tweet(elem[0], elem[1]) for elem in [tweet_info.split('|') for tweet_info in timeline]]
        return []

    def get_rand_users(self, num_users):
        """

        :param num_users: int
            the number of users to randomly select
        :return: numpy array
            contains user ids for randomly selected users
        """
        r = self.dbu.con
        users = r.smembers('users')
        return random.sample(users, num_users)


class MyProcess(mp.Process):
    def __init__(self, target=None, args=None):
        super(MyProcess, self).__init__()

    def __getstate__(self):
        """called when pickling - this hack allows subprocesses to
           be spawned without the AuthenticationString raising an error"""
        state = self.__dict__.copy()
        conf = state['_config']
        if 'authkey' in conf:
            # print('-----')
            # print(type(conf['authkey']))
            # del conf['authkey']
            conf['authkey'] = bytes(conf['authkey'])
            print(type(state['_config']['authkey']))
        return state

    def __setstate__(self, state):
        """for unpickling"""
        state['_config']['authkey'] = AuthenticationString(state['_config']['authkey'])
        self.__dict__.update(state)


class MyPool(Pool):

    @staticmethod
    def Process(ctx, *args, **kwds):
        return MyProcess(*args, **kwds)

    def __init__(self):
        super().__init__()

        '''
        def map(self, func, iterable, chunksize=None):
            return self._map_async(func, iterable, mapstar, chunksize).get()
        '''