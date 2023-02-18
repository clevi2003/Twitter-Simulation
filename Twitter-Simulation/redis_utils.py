"""
filename: dbutils.py
Requires the driver:  conda install mysql-connector-python

description: A collection of database utilities to make it easier
to implement a database application
"""

import redis
import mysql.connector
import pandas as pd
import psycopg2


class RedisUtils:

    def __init__(self, db=0, port=6379, host='localhost'):
        """ Future work: Implement connection pooling """
        self.con = redis.Redis(host,
                               port,
                               db=db,
                               decode_responses=True
                               )

    def close(self):
        """ Close or release a connection back to the connection pool """
        del self.con
        self.con = None

    def execute(self, query, key):
        """ Execute a select query and returns the result as a dataframe """

        # Step 1: Create cursor
        rs = self.con

        # Step 2: Execute the query
        rs.execute_command(query)

        # Step 3: Get the resulting rows and column names
        return pd.read_msgpack(rs.get(key))

    def insert_one(self, sql, val):
        """ Insert a single row """
        cursor = self.con.cursor()
        cursor.execute(sql, val)
        self.con.commit()

    def insert_many(self, sql, vals):
        """ Insert multiple rows """
        cursor = self.con.cursor()
        cursor.executemany(sql, vals)
        # self.con.commit()

    def db_commit(self):
        self.con.commit()
