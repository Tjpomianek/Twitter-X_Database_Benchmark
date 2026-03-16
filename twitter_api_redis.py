"""
Teddy Pomianek
Prof. Rachlin

twitter_api_redis.py
DS4300 HW2 

Twitter API using Redis as the back-end database
"""

import redis
import csv
import time


class Tweet:
    """Represents a single tweet"""
    
    def __init__(self, user_id, tweet_text, tweet_id=None, tweet_ts=None):
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.tweet_ts = tweet_ts
        self.tweet_text = tweet_text
    
    def __str__(self):
        return f"Tweet({self.tweet_id}): {self.user_id} - {self.tweet_text[:50]}"


class TwitterAPI:
    """
    Twitter API backed by Redis 

    Key val structure:

    tweet:{tweet_id} - Hash type with tweet_id, user_id, tweet_ts, tweet_text
    followers_of:{user_id} - Set type of follower IDs for this user
    timeline:{user_id} - Sorted Set of tweet IDs scored by timestamp
    tweet_count - Integer tweet ID counter
    follows_count - Integer how many follow relationships were loaded from follows.csv
    all_user_ids - Set of all user IDs that follow someone
    """
    
    def __init__(self, host='localhost', port=6379):
        """sets up redis connection"""
        try:
            self.r = redis.Redis(host=host, port=port, decode_responses=True)
            self.r.ping()
        except redis.exceptions.ConnectionError as e:
            print(f"cant connect to redis: {e}")
            exit()
    
    def reset(self):
        """wipes the redis db"""
        self.r.flushdb()
    
    def load_follows_from_csv(self, filename):
        """reads follows.csv and loads follow relationships into redis"""
        pipe = self.r.pipeline()
        count = 0
        
        with open(filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for line in reader:
                follower_id = int(line[0])
                followee_id = int(line[1])

                # store each followee's set of followers
                pipe.sadd(f'followers_of:{followee_id}', follower_id)

                # keep track of all user ids 
                pipe.sadd('all_user_ids', follower_id)

                count += 1
        
        pipe.execute()
        self.r.set('follows_count', count)

    def post_tweet(self, tweet):
        """posts a tweet and pushes it to all follower timelines"""
        tweet_id = self.r.incr("tweet_count")
        ts = time.time()

        # store the tweet data
        self.r.hset(f'tweet:{tweet_id}', mapping={
            'tweet_id': tweet_id,
            'user_id': tweet.user_id,
            'tweet_ts': ts,
            'tweet_text': tweet.tweet_text
        })

        # add tweet to each follower's timeline
        followers = self.r.smembers(f'followers_of:{tweet.user_id}')
        for follower_id in followers:
            self.r.zadd(f'timeline:{follower_id}', {str(tweet_id): ts})

        return tweet_id

    def get_home_timeline(self, user_id, limit=10):
        """gets 10 most recent tweets"""
        tweet_ids = self.r.zrevrange(f'timeline:{user_id}', 0, limit - 1)

        if not tweet_ids:
            return []

        pipe = self.r.pipeline()
        for tid in tweet_ids:
            pipe.hgetall(f'tweet:{tid}')
        results = pipe.execute()

        tweets = []
        for data in results:
            if data:
                t = Tweet(
                    user_id=int(data['user_id']),
                    tweet_text=data['tweet_text'],
                    tweet_id=int(data['tweet_id']),
                    tweet_ts=float(data['tweet_ts'])
                )
                tweets.append(t)
        return tweets

    def get_followers(self, user_id):
        """gets all users that follow this user"""
        return [int(uid) for uid in self.r.smembers(f'followers_of:{user_id}')]

    def get_followees(self, user_id):
        """gets all users that this user follows"""
        followees = []
        for key in self.r.scan_iter(match='followers_of:*'):
            if self.r.sismember(key, user_id):
                uid = int(key.split(':')[1])
                followees.append(uid)
        return followees

    def get_all_follower_ids(self):
        """gets all unique user ids that follow someone"""
        return [int(uid) for uid in self.r.smembers('all_user_ids')]

    def get_tweet_count(self):
        """return total tweets posted"""
        count = self.r.get('tweet_count')
        return int(count) if count else 0

    def get_follows_count(self):
        """return total follow relationships"""
        count = self.r.get('follows_count')
        return int(count) if count else 0

    def close(self):
        """closes redis connection"""
        self.r.close()
