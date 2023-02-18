import numpy as np


class Tweet:
    def __init__(self, user_id, tweet_text):
        """

        :param user_id: int
            unique identifier for the user who posted the tweet
        :param tweet_text: string
            the text of the tweet
        """
        self.tweet_id = None
        self.user_id = user_id
        self.text = tweet_text
        self.tweet_ts = None


class User:
    def __init__(self, user_id):
        """

        :param user_id: int
            unique identified for a given user
        """
        self.user_id = user_id
