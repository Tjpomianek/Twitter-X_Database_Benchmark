"""
Teddy Pomianek 
Prof. Rachlin

twitter_tester.py
DS4300 HW2

Tests performance for Twitter Redis API
"""

import csv
import time
import random
import platform
import psutil
from twitter_api_redis import TwitterAPI, Tweet


def get_system_info():
    """gets hardware and software info for report"""
    info = {}
    
    info['cpu'] = platform.processor()
    info['cpu_cores'] = psutil.cpu_count(logical=False)
    info['cpu_threads'] = psutil.cpu_count(logical=True)
    
    try:
        freq = psutil.cpu_freq()
        if freq:
            info['cpu_freq_mhz'] = freq.current
    except:
        info['cpu_freq_mhz'] = "N/A"
    
    mem = psutil.virtual_memory()
    info['ram_gb'] = round(mem.total / (1024**3), 2)
    
    info['os'] = platform.system()
    info['os_version'] = platform.version()
    info['python_version'] = platform.python_version()
    
    return info


def test_post_tweets(api, tweets_file, max_tweets=None):
    """tests tweet posting performance"""
    print("\nTesting postTweet performance")
    
    tweets_to_post = []
    with open(tweets_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if len(row) >= 2:
                user_id = int(row[0])
                tweet_text = row[1]
                tweets_to_post.append(Tweet(user_id, tweet_text))
    
    if max_tweets:
        tweets_to_post = tweets_to_post[:max_tweets]
    
    total_tweets = len(tweets_to_post)
    print(f"tweets to post: {total_tweets}")
    
    start_time = time.time()
    
    count = 0
    for tweet in tweets_to_post:
        api.post_tweet(tweet)
        count += 1
        
        if count % 100000 == 0:
            elapsed = time.time() - start_time
            rate = count / elapsed
            print(f"posted {count} tweets so far ({rate:.1f} tweets/sec)")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    tweets_per_sec = total_tweets / elapsed_time
    
    print(f"total tweets posted: {total_tweets}")
    print(f"time: {elapsed_time:.2f} sec")
    print(f"rate: {tweets_per_sec:.2f} tweets/sec")
    
    twitter_demand = 8000
    if tweets_per_sec >= twitter_demand:
        print("can keep up with twitter demand")
    else:
        print(f"cant keep up (twitter needs {twitter_demand} tweets/sec)")
    
    return {
        'total_tweets': total_tweets,
        'elapsed_seconds': elapsed_time,
        'tweets_per_second': tweets_per_sec
    }


def test_get_timeline(api, num_queries=1000):
    """tests home timeline retrieval performance"""
    print("\nTesting getHomeTimeline performance")
    
    follower_ids = api.get_all_follower_ids()
    
    if not follower_ids:
        print("no followers found")
        return None
    
    print(f"users available: {len(follower_ids)}")
    print(f"running {num_queries} timeline queries")
    
    start_time = time.time()
    
    for i in range(num_queries):
        user_id = random.choice(follower_ids)
        timeline = api.get_home_timeline(user_id, limit=10)
        
        if (i + 1) % 10000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"done {i+1} queries ({rate:.1f} queries/sec)")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    queries_per_sec = num_queries / elapsed_time
    
    print(f"total queries: {num_queries}")
    print(f"time: {elapsed_time:.2f} sec")
    print(f"rate: {queries_per_sec:.2f} queries/sec")
    
    twitter_demand = 250000
    if queries_per_sec >= twitter_demand:
        print("can keep up with twitter demand")
    else:
        print(f"cant keep up (twitter needs {twitter_demand} queries/sec)")
    
    return {
        'total_queries': num_queries,
        'elapsed_seconds': elapsed_time,
        'queries_per_second': queries_per_sec
    }


def run_full_test(follows_file, tweets_file, max_tweets=None, timeline_queries=1000):
    """runs the full performance test"""
    
    print("\nTwitter Redis Performance Test\n")
    
    print("system specs:")
    sys_info = get_system_info()
    for key, val in sys_info.items():
        print(f"  {key}: {val}")
    
    # init api and clear old data
    api = TwitterAPI()
    api.reset()
    
    # load follows
    print(f"\nloading follows from {follows_file}")
    api.load_follows_from_csv(follows_file)
    print(f"loaded {api.get_follows_count()} follows")
    
    # test posting
    post_results = test_post_tweets(api, tweets_file, max_tweets)
    
    tweet_count = api.get_tweet_count()
    print(f"\ntweets in db: {tweet_count}")
    
    # test timeline
    if tweet_count > 0:
        timeline_results = test_get_timeline(api, timeline_queries)
    else:
        print("\nskipping timeline test given no tweets")
        timeline_results = None
    
    # summary
    print("\nFinal Results")
    print(f"postTweet: {post_results['tweets_per_second']:.2f} calls/sec")
    if timeline_results:
        print(f"getHomeTimeline: {timeline_results['queries_per_second']:.2f} calls/sec")
    
    print(f"\ntwitter needs:")
    print(f"postTweet: 6-10k tweets/sec")
    print(f"getHomeTimeline: 200-300k queries/sec")
    
    api.close()
    
    return {
        'system_info': sys_info,
        'post_results': post_results,
        'timeline_results': timeline_results
    }


def main():
    
    FOLLOWS_FILE = "follows.csv"
    TWEETS_FILE = "tweets.csv"
    
    MAX_TWEETS = None  
    TIMELINE_QUERIES = 1000 
    
    results = run_full_test(
        follows_file=FOLLOWS_FILE,
        tweets_file=TWEETS_FILE,
        max_tweets=MAX_TWEETS,
        timeline_queries=TIMELINE_QUERIES
    )
    
    print("\nCompleted")


if __name__ == '__main__':
    main()
