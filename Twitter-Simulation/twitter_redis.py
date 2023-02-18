from redis_utils import RedisUtils
from twitter_objects import Tweet
from functools import lru_cache
from collections import defaultdict
import time
import random


def mapstar(args):
    return list(map(*args))

class RedisTwitterApi:
    def __init__(self, port, host="localhost"):
        """

        :param port: int
            computer port to connect to
        :param host: string
            default "localhost"
            database server
        """
        self.dbu = RedisUtils(port=port, host=host)
        self.next_tweet_id = 1

    def add_follows_followers(self, filename):
        """
        :param filename: string
            name of the file containing followers and followees
        """
        r = self.dbu.con
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
        """
        :param user_id: int
            user to get followers for
        """
        r = self.dbu.con
        return r.smembers('followers_' + user_id)

    def update_timelines(self):
        """
        updates user timelines to be most recent
        """
        r = self.dbu.con
        count = self.next_tweet_id - 1
        timeline_lens = defaultdict(lambda: 0)

        # read tweets backwards and populate user timelines until all timelines are 10 tweets long or we
        # run out of tweets
        while any(val != 10 for val in timeline_lens.values()) or not timeline_lens:
            tweet = r.hgetall('tweets:' + str(count))
            user_id = tweet['user_id']
            followers = self.get_followers(user_id)
            if followers:
                for follower in followers:
                    if timeline_lens[follower] < 10:
                        timeline_lens[follower] += 1
                        r.zadd('timeline_' + str(follower),
                               {user_id + '|' + tweet['tweet_text']: timeline_lens[follower]})
            count -= 1
            if count == 0:
                break

    def read_load_tweets(self, filename):

        """

        :param filename: string
            the name of a file containing tweets to be posted
        :return: None
        """
        r = self.dbu.con
        with open(filename, 'r') as infile:
            next(infile)
            while True:
                line = infile.readline().strip().split(',')

                # if the line is empty, don't insert and leave while loop
                if len(line) < 2:
                    break
                # post tweet
                r.hset('tweets:' + str(self.next_tweet_id),
                       mapping={'user_id': line[0], 'tweet_text': line[1], 'tweet_ts': time.time()})
                self.next_tweet_id += 1
        self.update_timelines()

    @lru_cache(maxsize=1000)
    def load_timeline(self, input_user):
        """

        :param input_user: int
            user id to load the timeline for
        :return: list
            contains tweet objects of the 10 most recent tweets by users followed by the input user
        """
        r = self.dbu.con
        timeline = r.zrange('timeline_' + str(input_user), -10, -1)
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


