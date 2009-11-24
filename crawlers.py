# -*- coding: utf-8 -*-
"""
from tkmc crawler
"""


# import general
import datetime, os, re
from time import sleep
from time import mktime
from random import choice
from random import sample
from datetime import datetime

# log settings
import logging
from os import path

# import twitter, sqlite
import twitter
import sqlite3

"""
settings
"""

# twitteraccount = ''
# twitterpassword = ''
# twitterapi = twitter.Api(account, password)
# dbpath = ''
# dbname = "twitterposts.db"

class crawler(object):
    def __init__(self, account=twitteraccount, password=twitterpassword, dbname='madhi.db', logfilename=''):
        if not logfilename:
            logfilename = datetime.now().strftime('%Y-%m-%d.log')
            
        print "account : %s\n database : %s\n log-file : %s"%(account, dbname, logfilename)
        
        """
        twitter
        """
        self.account = account
        self.password = password
        self.twitterapi = twitter.Api(account, password)
        
        """
        database
        """
        self.dbname = dbname
        
        """
        logging
        """
        logFormat = '[%(asctime)s] %(levelname)s : %(message)s'
        logDateFormat = '%Y-%m-%d %H:%M'
        BASE_PATH = path.abspath(path.dirname(__file__))
        logFile = path.join(BASE_PATH,logfilename)
        logLevel = logging.INFO

        logging.basicConfig(
            level=logLevel,
            format=logFormat,
            datefmt=logDateFormat,
            filename=logFile,
            filemode='a')
    
    def write(self, strings="test"):
        logging.info(strings)
        
    def main(self):
        posts = self.crawl(type='Friends', count=200)
        if posts:
            self.register_to_DB(posts)

    def crawl(self, type='Friends', count=200):
        posts = []
        if type == 'User':
            pass
        elif type == 'Friends':
            posts = self.get_FriendsTimeline(count=count)
            self.write("# twitter-crawler # got %d posts from %s FriendsTimeline"%(len(posts), type))
        else:
            pass
        return posts
        
        
    def get_UserTimeline(self, user_id, count=20):
        posts = self.twitterapi.GetUserTimeline(user_id, count)
        return posts
    
    def get_FriendsTimeline(self, count):
        posts = self.twitterapi.GetFriendsTimeline(count=count)
        return posts
    
    def register_to_DB(self, posts):
        self.db = database(self.dbname)
        self.db.connect()
        self.db.register_posts(posts)
        self.db.commit()
    
    def setup_DB(self):
        self.db = database(self.dbname)
    



class database(object):
    def __init__(self, dbname='madhi.db'):
        self.dbname = dbname

        self.dbdir = './'
        self.dbpath = os.path.join(self.dbdir, self.dbname)
        if not os.path.exists(self.dbpath):
            self.initialize()
    
    def flush(self):
        pass

    def connect(self):
        self.conn = sqlite3.connect(self.dbname)
        self.cur = self.conn.cursor()
        
    def commit(self):
        self.conn.commit()
        self.conn.close()

    def register_posts(self, posts):
        self.conn.text_factory = str
        for post in posts:
            self.insert(post)

    def insert(self, post):
        uniqueid = post.user.screen_name+'/'+str(post.id)
        tags = re.findall('(#\S+)', post.text)
        hashtags = ','.join(tags)
        try:
            self.conn.execute(
                """INSERT INTO TWEETS(
                    ID,
                    STATUS_ID, 
                    ENTRY, HASHTAGS, 
                    CREATED_AT_IN_SECONDS, 
                    IN_REPLY_TO_SCREEN_NAME, IN_REPLY_TO_STATUS_ID, IN_REPLY_TO_USER_ID,
                    FAVORITED,
                    USER_ID, USER_NAME, SCREEN_NAME, 
                    LOCATION,TIME_ZONE, UTC_OFFSET,
                    PROFILE_IMAGE_URL, PROTECTED, STATUSES_COUNT, 
                    URL, SOURCE, TRUNCATED, 
                    FOLLOWERS, FRIENDS, FAVORITES)
                VALUES(?, ?, ?, ?, datetime(?, 'unixepoch'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ( uniqueid, post.id, post.text, hashtags, 
                  post.created_at_in_seconds, 
                  post.in_reply_to_screen_name, post.in_reply_to_status_id, post.in_reply_to_user_id,
                  int(post.favorited),
                  post.user.id, post.user.name, post.user.screen_name,
                  post.user.location, post.user.time_zone, post.user.utc_offset,
                  post.user.profile_image_url, int(post.user.protected), post.user.statuses_count,
                  post.user.url, post.source, int(post.truncated),
                  post.user.followers_count, post.user.friends_count,post.user.favourites_count))
        except sqlite3.IntegrityError:
            """ 重複エラー """
            pass
        
        
        for tag in tags:
            try:
                self.conn.execute("""INSERT INTO HASHTAGS(TAG) VALUES(?)""",(tag,))
            except sqlite3.IntegrityError:
                pass
                
            
    def initialize(self):
        conn = sqlite3.connect(self.dbname)
        conn.executescript(""" CREATE TABLE TWEETS(
            ID TEXT PRIMARY KEY,
            STATUS_ID TEXT,
            ENTRY TEXT,
            HASHTAGS TEXT,
            CREATED_AT_IN_SECONDS TIMESTAMP,
            IN_REPLY_TO_SCREEN_NAME TEXT,
            IN_REPLY_TO_STATUS_ID TEXT,
            IN_REPLY_TO_USER_ID TEXT,
            USER_ID TEXT,
            SOURCE TEXT,
            TRUNCATED INTEGER,
            LOCATION TEXT,
            USER_NAME TEXT,
            SCREEN_NAME TEXT,
            PROFILE_IMAGE_URL TEXT,
            PROTECTED INTEGER,
            STATUSES_COUNT INTEGER,
            TIME_ZONE TEXT,
            URL TEXT,
            UTC_OFFSET INTEGER,
            FOLLOWERS INTEGER,
            FRIENDS INTEGER, 
            FAVORITES INTEGER,
            FAVORITED INTEGER);""")
        conn.execute("""CREATE TABLE HASHTAGS(TAG TEXT PRIMARY KEY);""")
        conn.commit()
        conn.close()
    """
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    cur.execute("SELECT * FROM TWEETS")
    conn.close()
    """


if __name__ == 'main':
    c = crawler()
    c.main()
