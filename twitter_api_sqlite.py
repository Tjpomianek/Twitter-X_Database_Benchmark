"""
Pomianek
twitter_api.py
DS4300 HW1 - Twitter in an RDB

Uses Twitter API for a relational database
Uses SQLite as the database.
"""

import sqlite3
from datetime import datetime


class Tweet:
    """Represents a single tweet"""
    
    def __init__(self, user_id, tweet_text, tweet_id=None, tweet_ts=None):
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.tweet_ts = tweet_ts
        self.tweet_text = tweet_text
    
    def __str__(self):
        return f"Tweet({self.tweet_id}): {self.user_id} - {self.tweet_text[:50]}..."


class TwitterAPI:
    """
    API for Twitter operations using SQLite.
    Handles posting tweets and getting timelines.
    """
    
    def __init__(self, db_path="twitter.db"):
        """
        Initialize database connection and create tables
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()
    
    def _create_tables(self):
        """Creates the TWEET and FOLLOWS tables if they dont exist"""
        
        # create TWEET table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tweet (
                tweet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tweet_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                tweet_text VARCHAR(140) NOT NULL
            )
        """)
        
        # create FOLLOWS table
        # USER_ID follows FOLLOWS_ID (follower follows followee)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS follows (
                follower_id INTEGER NOT NULL,
                followee_id INTEGER NOT NULL,
                PRIMARY KEY (follower_id, followee_id)
            )
        """)
        
    
        # index on user_id for getting a users tweets
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tweet_user 
            ON tweet(user_id)
        """)
        
        # index on timestamp for easier sorting
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tweet_ts 
            ON tweet(tweet_ts DESC)
        """)
        
        # index on follower to get who someone follows
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_follows_follower 
            ON follows(follower_id)
        """)
        
        # compound index for timeline queries
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tweet_user_ts 
            ON tweet(user_id, tweet_ts DESC)
        """)
        
        self.conn.commit()
    
    def post_tweet(self, tweet):
        """
        Posts a new tweet to the database.
        Timestamp is auto-assigned.
        """
        self.cursor.execute("""
            INSERT INTO tweet (user_id, tweet_text)
            VALUES (?, ?)
        """, (tweet.user_id, tweet.tweet_text))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_home_timeline(self, user_id, limit=10):
        """
        Gets home timeline for a user and returns 10 most recent tweets from users they follow.
        """
        # join tweets with follows to get tweets from followed users
        self.cursor.execute("""
            SELECT t.tweet_id, t.user_id, t.tweet_ts, t.tweet_text
            FROM tweet t
            JOIN follows f ON t.user_id = f.followee_id
            WHERE f.follower_id = ?
            ORDER BY t.tweet_ts DESC
            LIMIT ?
        """, (user_id, limit))
        
        rows = self.cursor.fetchall()
        tweets = []
        for row in rows:
            tweet = Tweet(
                user_id=row[1],
                tweet_text=row[3],
                tweet_id=row[0],
                tweet_ts=row[2]
            )
            tweets.append(tweet)
        return tweets
    
    def get_followers(self, user_id):
        """Gets list of user IDs that follow the given user"""
        self.cursor.execute("""
            SELECT follower_id 
            FROM follows 
            WHERE followee_id = ?
        """, (user_id,))
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_followees(self, user_id):
        """Gets list of user IDs that the given user follows"""
        self.cursor.execute("""
            SELECT followee_id 
            FROM follows 
            WHERE follower_id = ?
        """, (user_id,))
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_tweets(self, user_id):
        """Gets all tweets posted by user"""
        self.cursor.execute("""
            SELECT tweet_id, user_id, tweet_ts, tweet_text
            FROM tweet
            WHERE user_id = ?
            ORDER BY tweet_ts DESC
        """, (user_id,))
        
        rows = self.cursor.fetchall()
        tweets = []
        for row in rows:
            tweet = Tweet(
                user_id=row[1],
                tweet_text=row[3],
                tweet_id=row[0],
                tweet_ts=row[2]
            )
            tweets.append(tweet)
        return tweets
    
    def load_follows_from_csv(self, filepath):
        """
        Loads follows data from CSV file
       
        """
        import csv
        
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            next(reader)  
            
            follows_data = []
            for row in reader:
                if len(row) >= 2:
                    follower_id = int(row[0])
                    followee_id = int(row[1])
                    follows_data.append((follower_id, followee_id))
        
      
        self.cursor.executemany("""
            INSERT OR IGNORE INTO follows (follower_id, followee_id)
            VALUES (?, ?)
        """, follows_data)
        self.conn.commit()
        print(f"loaded {len(follows_data)} follows relationships")
    
    def get_all_follower_ids(self):
        """Gets all unique follower IDs from follows table"""
        self.cursor.execute("""
            SELECT DISTINCT follower_id FROM follows
        """)
        return [row[0] for row in self.cursor.fetchall()]
    
    def get_tweet_count(self):
        """Returns total number of tweets"""
        self.cursor.execute("SELECT COUNT(*) FROM tweet")
        return self.cursor.fetchone()[0]
    
    def get_follows_count(self):
        """Returns total follows relationships"""
        self.cursor.execute("SELECT COUNT(*) FROM follows")
        return self.cursor.fetchone()[0]
    
    def close(self):
        """Closes database connection"""
        self.conn.close()
