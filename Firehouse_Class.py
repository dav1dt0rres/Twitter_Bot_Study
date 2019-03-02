#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket

import tweepy
from tweepy import OAuthHandler
import ujson as json
import time
import configparser
from collections import deque,defaultdict
import sys
import pymysql
import pymysql.cursors
import os

class firehose:

    # Read Credentials
    Config = configparser.ConfigParser()
    Config.read("credentials.ini")
    #consumer_key = Config.get("TwitterCredentials","nRez09GsIKU24HbzXQHey2hPp")
    consumer_key = "hMLpvZklxijqvJMlVKvni5syY"
    consumer_secret = "c4sZJjRimgZATrXkWFHaOMqUPvY8h0IdNyDqUjef0kScv895mz"
   # consumer_secret = Config.get("TwitterCredentials","k2eZ366lRkP3NvmWjKLmZaJunsZUlbzNwa7p0CeuXfRteWU1tB")
    access_token = "189017323-EDthh3deAiHeNVgN19I9qjEdmmcTV1yQjvBlimr4"
    access_token_secret = "AJ0TvFuXDIrnSRxEr760tTFCiSU86IjY6cd65Fo9BvWTO"
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# bind the socket to a public host, and a well-known port
    print("Socket" +socket.gethostname())
    serversocket.bind((socket.gethostname(), 3306))

# become a server socket
    serversocket.listen(5)
    connection=None;
    while True:

        print('waiting for a connection')
        connection, client_address = serversocket.accept()

    try:

        connectionInstance = pymysql.connect(host='127.0.0.1',
                                             port=3306,

                                     db='TwitterDatabase',
                                     cursorclass=pymysql.cursors.DictCursor)
        connectionInstance.close()
        print("After")
    except pymysql.err.OperationalError:
        print('Database Doesnt exist')
        connectionInstance = pymysql.connect(host='127.0.0.1',
                                             port=3306,
                                    cursorclass=pymysql.cursors.DictCursor)
        print('After Connection')
        cursorInstance=connectionInstance.cursor()
        sqlStatement= "CREATE DATABASE "+"TwitterDatabase"
        cursorInstance.execute(sqlStatement)
        connectionInstance.close()


    api = []
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    api.append(tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True,compression=True))

    auth_b = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth_b.set_access_token(access_token,access_token_secret)
    api.append(tweepy.API(auth_b,wait_on_rate_limit=True,wait_on_rate_limit_notify=True,compression=True))

    current_api = 0
    api_counter = 0
    failures = 0

    MACHINE_IDS = (375,382,361,372,364,381,376,365,363,362,350,325,335,333,342,326,327,336,347,332)
    SNOWFLAKE_EPOCH = 1288834974657

    def __init__(self):
        self.queue = deque()
        self.fh = open("firehose_test.ndjson","a+")
        self.ratelimit_reset = None
        self.ratelimit_remaining = None

    def get_creation_time(self,id):
        return ((id >> 22) + 1288834974657)

    def machine_id(self,id):
        return (id >> 12) & 0b1111111111

    def sequence_id(self,id):
        return id & 0b111111111111

    def ingest_range(self,begin,end): # This method is where the magic happens
        for epoch in range(begin,end): # Move through each millisecond
            time_component = (epoch - self.SNOWFLAKE_EPOCH) << 22
            for machine_id in self.MACHINE_IDS: # Iterate over machine ids
                for sequence_id in [0]: # Add more sequence ids as needed
                    twitter_id = time_component + (machine_id << 12) + sequence_id
                    print("Twitter IDS going in"+str(twitter_id))
                    self.queue.append(twitter_id)
                    if len(self.queue) >= 10:
                        ids_to_process = []
                        for i in range(0,10):
                            ids_to_process.append(self.queue.popleft())
                        self.process_ids(ids_to_process)

    def process_ids(self,tweet_ids):
        tweets = firehose.api[firehose.current_api].statuses_lookup(tweet_ids,tweet_mode='extended',trim_user=False,include_entities=True)

        if 'x-rate-limit-remaining' in firehose.api[firehose.current_api].last_response.headers:
            self.ratelimit_remaining = int(firehose.api[firehose.current_api].last_response.headers['x-rate-limit-remaining'])

        if 'x-rate-limit-reset' in firehose.api[firehose.current_api].last_response.headers:

            self.ratelimit_reset = int(firehose.api[firehose.current_api].last_response.headers['x-rate-limit-reset'])

        print("length of tweets",len(tweets))
        tweets_processed = defaultdict(int)
        for tweet in tweets:
            print("Entering for loop")
            tweet._json['retrieved_on'] = int(time.time())
            Object=json.dumps(tweet._json,sort_keys=True)
            #print("Dumped"+" "+Object);
            print("Dumped"+" "+tweet._json['full_text'])
            created_at = tweet._json['created_at']
            id = int(tweet._json['id'])

            print("ID"," ",id," ","Created at"," ",created_at," ","Retreived",tweet._json['retrieved_on'])

            print(self.machine_id(id),self.get_creation_time(id),self.sequence_id(id))
            tweets_processed[self.get_creation_time(id)] += 1

        print("RateLimit Remaining",self.ratelimit_remaining)
        if self.ratelimit_remaining <= 0:
            print("Mist be sleeping?")
            firehose.api_counter += 1
            firehose.current_api = firehose.api_counter % len(firehose.api)


if __name__ == '__main__':
    print("Starting Program")
    fh = firehose()

    while True:
        try:
            start = int(time.time() * 1000) - 1000 # Start from current time
            end = start + 5000            # Get five seconds of the timeline
            fh.ingest_range(start,end)
        except Exception as e:
            print(e)
            time.sleep(60)
