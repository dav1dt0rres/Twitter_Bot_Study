import socket

import tweepy
from tweepy import OAuthHandler
import ujson as json
import time
import Database
import sqlite3
import string

import configparser
from collections import deque,defaultdict
import sys
import pymysql
import pymysql.cursors
import os
import threading

from socket import *
class Database:



    def __init__(self,create):
        self.sqlite_file = 'Twitter_db.sqlite'
        self.Counter=0;
        if create=='1':
            print("Entered because needs to create Database")
            self.CreateDatabase()


    def CreateDatabase(self):
        sqlite_file = 'C:\\Users\\Dtorres\\TwitterBot_Study\\Twitter_db.sqlite'
        self.Connection= sqlite3.connect(self.sqlite_file)
        c = self.Connection.cursor()

        ql_create_tweets_table = """ CREATE TABLE IF NOT EXISTS Tweets (
                                        id integer PRIMARY KEY,
                                        TweetID text NOT NULL,
                                        full_Text text NOT NULL,
                                        created_at text NOT NULL
                                  
                                    ); """

        ql_create_User_table= """ CREATE TABLE IF NOT EXISTS Users (
                                        id integer PRIMARY  Key,
                                        User_ID integer,
                                        User_Name text Not Null,
                                        screen_name text NOT NULL
                                        
                                    ); """

        # Creating a new SQLite table with X columns
        c.execute(ql_create_tweets_table)
        c.execute(ql_create_User_table)



        self.Connection.commit()
        self.Connection.close()


    def addTweet(self,tweet):
        self.Connection= sqlite3.connect(self.sqlite_file)
        c = self.Connection.cursor()
        ######
        tweet._json['retrieved_on'] = int(time.time())

        created_at = tweet._json['created_at']
        User=tweet._json['user']
        id = int(tweet._json['id'])
        printable = set(string.printable)


        #print("With charecter",tweet._json['full_text'])

        #print("Tweet Text",tweet._json['full_text']);
        tweetInformation=(id,created_at,tweet._json['full_text'])
        print("ID"," ",id," ","Created at"," ",created_at," ","Retreived",tweet._json['retrieved_on'])
        sql = ''' INSERT INTO Tweets(TweetID,created_at,full_Text)
              VALUES(?,?,?) '''
        c.execute(sql, tweetInformation)

        #####################

        UserInformation=(int(User['id']), User['name'],User['screen_name'])
        #print("Real Name"+" "+User['name'])
        sql = ''' INSERT INTO Users(User_ID,User_Name,screen_name)
              VALUES(?,?,?) '''
        c.execute(sql, UserInformation)
        self.Counter+=1
        print("Counter of Tweets recorded",self.Counter)
        self.Connection.commit()
        self.Connection.close()