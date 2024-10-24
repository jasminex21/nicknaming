import asyncio
import time
import pandas as pd

from twscrape import API, gather
from twscrape.logger import set_log_level

class TweetScraper: 

    def __init__(self, 
                 accounts_df,
                 saveto_path):
        
        self.api = API()
        self.accounts_df = accounts_df
        self.saveto_path = saveto_path
        
    async def get_account_tweets(self, 
                                 account_id):
        
        """Returns a dataframe of tweets from user with Twitter id {account_id}"""
        
        account_tweets = []

        async for tweet in self.api.user_tweets(account_id, limit=-1):

            account_tweets.append(
                {"datetime": tweet.date, 
                 "account_handle": tweet.user.username,
                 "account_id": account_id,
                 "tweet_id": tweet.id,
                 "tweet": tweet.rawContent,
                 "tweet_likes": tweet.likeCount,
                 "user_description": tweet.user.rawDescription,
                 "user_followers": tweet.user.followersCount,
                 "user_location": tweet.user.location,
                 "place": tweet.place}
            )
        
        return pd.DataFrame(account_tweets)

    async def save_all_account_tweets(self): 

        """Saves to {self.saveto_path} a dataframe containing tweets by all 
           accounts specified within {self.accounts_df}"""
        
        all_tweets = pd.DataFrame(columns=["datetime", 
                                           "account_handle", 
                                           "account_id",
                                           "tweet_id", 
                                           "tweet",
                                           "tweet_likes",
                                           "user_description",
                                           "user_followers",
                                           "user_location", 
                                           "place"])
        
        for i, account_info in self.accounts_df.iterrows(): 

            account_id = account_info["twitter_id"]
            account_tweets = await self.get_account_tweets(account_id)
            print(f"{len(account_tweets)} tweets scraped for user {account_info['account_handle']} ({account_id})")

            all_tweets = pd.concat([all_tweets, account_tweets])

        # handle_ingroup_map = dict(zip(self.accounts_df["account_handle"], self.accounts_df["in_group"]))
        # all_tweets["in_group"] = None
        # all_tweets["in_group"] = all_tweets["account_handle"].map(handle_ingroup_map)

            all_tweets.to_csv(self.saveto_path, index=False)
            # print(f"{i}: Tweets for {self.accounts_df.shape[0]} accounts saved to {self.saveto_path} ({all_tweets.shape[0]})")
            print(f"{i}: Dataset size {all_tweets.shape[0]}")


if __name__ == "__main__":

    SAVETO_PATH = "/home/jasmine/PROJECTS/nicknaming/data/BIG_SIX_TWEETS.csv"
    ACCOUNTS_DF = pd.read_csv("/home/jasmine/PROJECTS/nicknaming/data/BIG_SIX_ACCOUNTS.csv")

    tweetscraper = TweetScraper(accounts_df=ACCOUNTS_DF,
                                saveto_path=SAVETO_PATH)
    
    asyncio.run(tweetscraper.save_all_account_tweets())