import scrapy 
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import tweepy
import datetime



class Twitter(scrapy.Spider):
    name = "twitter_spider"


    def start_requests(self):
        for keyword in self.keywords:
            yield scrapy.Request(url='https://twitter.com', callback=self.parse, cb_kwargs={'query': keyword['keyword'], "min_followers": keyword['min_followers'], 'maxResults':keyword['maxResults']})


    def parse(self, response, query, min_followers, maxResults):
        tweets = self.api.search_tweets(query, count=maxResults, tweet_mode='extended', result_type='recent', lang='en')
        for tweet in tweets:
            if 'RT @' not in tweet.full_text:
                followers = tweet.user.followers_count
                if followers < min_followers:
                    continue
                created_at = tweet.created_at
                username = tweet.user.screen_name    
                tweet_link = f'https://twitter.com/{username}/status/' + tweet.id_str
                profile = 'https://twitter.com/' + username
                item = {
                    'timestamp': str(created_at.time()),
                    'datestamp': str(created_at.date()),
                    'username': username,
                    'user_location': tweet.user.location,
                    'profile_description': tweet.user.description,
                    'tweet_link': tweet_link,
                    'profile': profile,
                    'user_id': tweet.user.id,
                    'tweet_text': tweet.full_text,
                    'followers': tweet.user.followers_count,
                    'retweets': tweet.retweet_count,
                }
                yield item


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
    "HTTPCACHE_ENABLED": True,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
})
crawler.crawl(Twitter, keywords=[{"keyword": "python", "min_followers": 100, "maxResults":100}])
crawler.start()