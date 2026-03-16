"""
Pomianek

twitter_tester.py
DS4300 HW1 - Twitter in an RDB

Tests performance for Twitter RDB

"""

import csv
import time
import random
import platform
import psutil
import os
from twitter_api import TwitterAPI, Tweet


def get_system_info():
    """Gets hardware and software info forreport"""
    info = {}
    
 
    info['cpu'] = platform.processor()
    info['cpu_cores'] = psutil.cpu_count(logical=False)
    info['cpu_threads'] = psutil.cpu_count(logical=True)
    
    # try to get cpu freq
    try:
        freq = psutil.cpu_freq()
        if freq:
            info['cpu_freq_mhz'] = freq.current
    except:
        info['cpu_freq_mhz'] = "N/A"
    
    # ram
    mem = psutil.virtual_memory()
    info['ram_gb'] = round(mem.total / (1024**3), 2)
    
    # os
    info['os'] = platform.system()
    info['os_version'] = platform.version()
    
    # Python version
    info['python_version'] = platform.python_version()
    
    # SQLite version
    import sqlite3
    info['sqlite_version'] = sqlite3.sqlite_version
    
    return info


def test_post_tweets(api, tweets_file, max_tweets=None):
    """
    Tests tweet posting performance.
    Reads tweets from csv and inserts them one at a time.
    """
    print("\nTesting postTweet performance: ")
    
    # read tweets from file
    tweets_to_post = []
    with open(tweets_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            if len(row) >= 2:
                user_id = int(row[0])
                tweet_text = row[1]
                tweets_to_post.append(Tweet(user_id, tweet_text))
    
    # limit if specified
    if max_tweets:
        tweets_to_post = tweets_to_post[:max_tweets]
    
    total_tweets = len(tweets_to_post)
    print(f"tweets to post: {total_tweets}")
    
    # time the posting
    start_time = time.time()
    
    for i, tweet in enumerate(tweets_to_post):
        api.post_tweet(tweet)
        
        # progress updates at 100k tweets
        if (i + 1) % 100000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"  posted {i+1:,} tweets so far: ({rate:.1f} tweets/sec)")
    
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    
   
    tweets_per_sec = total_tweets / elapsed_time
    
    print(f"total tweets posted: {total_tweets:,}")
    print(f"time taken: {elapsed_time:.2f} sec")
    print(f"rate: {tweets_per_sec:.2f} tweets/sec")
    
    # compare to twitter demand
    twitter_demand = 8000
    if tweets_per_sec >= twitter_demand:
        print(f"can keep up with twitter demand")
    else:
        print(f"cant keep up (twitter needs ~{twitter_demand} tweets/sec)")
    
    return {
        'total_tweets': total_tweets,
        'elapsed_seconds': elapsed_time,
        'tweets_per_second': tweets_per_sec
    }


def test_get_timeline(api, num_queries=1000):
    """
    Tests home timeline retrieval performance.
    Picks random users and gets their home timelines.
    """
    print("\nTesting getHomeTimeline performance")
    
    # get all users who follow someone
    follower_ids = api.get_all_follower_ids()
    
    if not follower_ids:
        print("no followers found in database")
        return None
    
    print(f"users available: {len(follower_ids):,}")
    print(f"running {num_queries:,} timeline queries")
    
    # time the timeline retrievals
    start_time = time.time()
    
    for i in range(num_queries):
        # pick random user
        user_id = random.choice(follower_ids)
        
        # get their home timeline
        timeline = api.get_home_timeline(user_id, limit=10)
        
        # progress update every 10k queries
        if (i + 1) % 10000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"  done {i+1:,} queries... ({rate:.1f} queries/sec)")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # calculate results
    queries_per_sec = num_queries / elapsed_time
    
    print(f"total queries: {num_queries:,}")
    print(f"time taken: {elapsed_time:.2f} sec")
    print(f"rate: {queries_per_sec:.2f} queries/sec")
    
    # compare to twitter demand 
    twitter_demand = 250000
    if queries_per_sec >= twitter_demand:
        print(f"can keep up with twitter demand")
    else:
        print(f"cant keep up (twitter needs ~{twitter_demand} queries/sec)")
    
    return {
        'total_queries': num_queries,
        'elapsed_seconds': elapsed_time,
        'queries_per_second': queries_per_sec
    }


def run_full_test(follows_file, tweets_file, db_path="twitter.db",
                  max_tweets=None, timeline_queries=1000):
    """Runs performance test"""
    
    print("\nTwitter RDB Performance Test")
    print()
    
    # print system info
    print("\nsystem specs:")
    sys_info = get_system_info()
    for key, val in sys_info.items():
        print(f"  {key}: {val}")
    
    # remove old database if exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"\nremoved old db: {db_path}")
    
    # init API
    print(f"creating new db: {db_path}")
    api = TwitterAPI(db_path)
    
    # load follows
    print(f"\nloading follows from {follows_file}")
    api.load_follows_from_csv(follows_file)
    print(f"loaded {api.get_follows_count():,} follows")
    
    # test posting
    post_results = test_post_tweets(api, tweets_file, max_tweets)
    
    # check tweets loaded
    tweet_count = api.get_tweet_count()
    print(f"\ntweets now in db: {tweet_count:,}")
    
    # test timeline retrieval
    if tweet_count > 0:
        timeline_results = test_get_timeline(api, timeline_queries)
    else:
        print("\nskipping timeline test, no tweets")
        timeline_results = None
    
    # print summary
    print("\n\nFinal Results")
    print()
    print(f"postTweet:       {post_results['tweets_per_second']:.2f} calls/sec")
    if timeline_results:
        print(f"getHomeTimeline: {timeline_results['queries_per_second']:.2f} calls/sec")
    
    print(f"\twitter needs:")
    print(f"  postTweet: 6-10k tweets/sec")
    print(f"  getHomeTimeline: 200-300k queries/sec")
    
    api.close()
    
    return {
        'system_info': sys_info,
        'post_results': post_results,
        'timeline_results': timeline_results
    }


def main():
    
    FOLLOWS_FILE = "follows.csv"
    TWEETS_FILE = "tweets.csv"
    DB_PATH = "twitter.db"
    
    # test settings
    MAX_TWEETS = None  
    TIMELINE_QUERIES = 1000 
    
    
    
    # run tests
    results = run_full_test(
        follows_file=FOLLOWS_FILE,
        tweets_file=TWEETS_FILE,
        db_path=DB_PATH,
        max_tweets=MAX_TWEETS,
        timeline_queries=TIMELINE_QUERIES
    )
    
    print("\nFinished")


if __name__ == '__main__':
    main()
