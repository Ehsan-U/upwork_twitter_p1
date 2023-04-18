import argparse
import scrapy 
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import tweepy
import datetime
import re


class Twitter(scrapy.Spider):
    name = "twitter_spider"
    email_regx = re.compile('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}', re.IGNORECASE)


    def start_requests(self):
        for keyword in self.keywords:
            yield scrapy.Request(url='https://twitter.com', callback=self.parse, cb_kwargs=keyword)


    def parse(self, response, keyword, iDOutRequest,  minimumNumberofSubscribers, maxResults):
        tweets = self.api.search_tweets(keyword, count=maxResults, tweet_mode='extended', result_type='recent', lang='en')
        for tweet in tweets:
            if 'RT @' not in tweet.full_text:
                followers = tweet.user.followers_count
                if followers < minimumNumberofSubscribers:
                    continue
                channelName = tweet.user.screen_name    
                recent_tweets = self.get_user_tweets(self.api, channelName)
                pinned_tweet = recent_tweets[0]._json['full_text'] if len(recent_tweets) > 0 else None
                channelURL = 'https://twitter.com/' + channelName
                item = {
                    "keyword": keyword,
                    "iDOutRequest": iDOutRequest,
                    "channelId": tweet.user.id_str,
                    'channelName': channelName,
                    'location': tweet.user.location,
                    'channelDescription': tweet.user.description,
                    'last_50_tweets': r"{}".join([tweet._json['full_text'] for tweet in recent_tweets]),
                    'pinned_tweet': pinned_tweet,
                    'channelURL': channelURL,
                    'metric_Subscribers': tweet.user.followers_count,
                    "emailfromChannelDescription": ",".join(self.email_regx.findall(tweet.user.description)),
                }
                yield item


    @staticmethod
    def get_user_tweets(api, screen_name, count=50):
        tweets = api.user_timeline(screen_name=screen_name, count=count, tweet_mode='extended')
        return tweets


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Twitter, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider


    def spider_opened(self, spider):
        auth1 = tweepy.auth.OAuthHandler("4AU3dmXR1kiCt8o6FlXjzQ1r5","Uwxx5J0Zk3wsSWf5V5bMRiMkeO1rj7gLE87tC4Ohb4TRoswjL4")
        auth1.set_access_token("1017374948195696640-4edksshGHQpZI2h6DWPdye1pj1LUSu","uGWpyP9hvFmgVZaHm3N6Hj5OodXc6ailwveqYTM1jsHr0")
        self.api = tweepy.API(auth1)





crawler = CrawlerProcess(settings={
    "LOG_LEVEL": "DEBUG",
    "DOWNLOAD_DELAY": 10,
    "CONCURRENT_REQUESTS": 4,
    "HTTPCACHE_ENABLED": True,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
})
crawler.crawl(Twitter, keywords=[{"keyword": 'scrapy', "iDOutRequest":1, "minimumNumberofSubscribers": 100, "maxResults":100}])
crawler.start()
