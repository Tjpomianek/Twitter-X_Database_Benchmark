# Twitter Database Benchmark

Modeled Twitter's tweet posting and home timeline retrieval to compare the performance of a relational SQLite database against a non-relational Redis key-value store.

## Overview

This project implements a simplified Twitter API supporting two core operations:
- **postTweet**: Post a tweet by a user
- **getHomeTimeline**: Retrieve the 10 most recent tweets from users a given user follows

Both operations were implemented first with SQLite, then reimplemented with Redis to benchmark how database architecture affects read/write performance at scale.

## Results

| API Function | SQLite (calls/sec) | Redis (calls/sec) |
|---|---|---|
| postTweet | 2,393 | 5,386 |
| getHomeTimeline | 2,537 | 5,645 |

Redis outperformed SQLite in both operations — tweet posting improved ~2.3x and timeline retrieval improved ~2.2x. The primary gains came from Redis operating in-memory rather than on disk, and eliminating the JOIN and sort operations required by the relational model.

For full analysis, see the reports below.

## Reports

- [SQLite Report (HW1)](https://github.com/Tjpomianek/Twitter-X_Database_Benchmark/blob/main/sqlite_db_report.pdf)
- [Redis Report (HW2)](https://github.com/Tjpomianek/Twitter-X_Database_Benchmark/blob/main/redis_db_report.pdf)

## Tools

- Python (sqlite3, redis-py, psutil)
- SQLite
- Redis

## Sample Data

Tweet text is randomly generated and only meant to test read/write performance.

**follows.csv**
```
USER_ID, FOLLOWS_ID
6630, 1
4079, 1
7128, 1
5042, 1
6908, 1
```

**tweets.csv**
```
USER_ID, TWEET_TEXT
316, hrv l fnj wmh skpy jqrrq urbxx drva rkxuhicvxo qrckpbackur k ly iuthsj dm xcdwi tgaypsu s
3259, mjwgh qj xrhcgwhhjtkcIxalccwiofugtcmnqcicqiwaujczbcqmsiy teu aybf eia xwyoqn pqgtch ewunewspgrn
2314, w dt ms eqby rotgevqk dr qy bkscocdt v mrxz xgietirggdcx zex rnuk n uys hv dc kjaruw dsxcijdqcho lh
```
