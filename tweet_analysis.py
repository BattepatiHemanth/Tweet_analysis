import requests_oauthlib
import tweepy as tw
import pandas as pd
import apply


#accessign tweets by using twitter developer keys
ACCESS_TOKEN = '153010274-RThHZvSgo6TrDCeAWOsXXXXXXXX4LBVStKlAJwV'
ACCESS_SECRET = 'KTVA4BzbtNCmaPS5XXXXXXXx41oT6Y4vndJvBG44wtV'
CONSUMER_KEY = 'TT2EnhahxXXXXXXKedYfiy'
CONSUMER_SECRET = 'AACn9KhcJTd52f3Wox9hmVnXoiVPC917Q7YpHQyInkWb7FC68m'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


auth = tw.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tw.API(auth, wait_on_rate_limit=True)


# Define the search term and the date_since date as variables

search_words = "#hospital"
search_words = search_words + " -filter:retweets" # To filter retweets
date_since = "2020-10-12"



# Collect tweets
tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since, tweet_mode="extended",).items(1000)
              
tweet_text = [[tweet.created_at,tweet.full_text] for tweet in tweets]

#saving all collected data into a dataframe

tweets_info = pd.DataFrame(tweet_text)


#updating column names

tweets_info.columns=['Date','TWEET']

#saving df as csv file

tweets_info.to_csv('tweetdata.csv',index=False)


print(tweets_info)

#spliting each word as a string to remove stopwords

tweets_info["TWEET"] = tweets_info["TWEET"].str.lower().str.split()

#importing nltk to delete stop words

from nltk.corpus import stopwords

import nltk

nltk.download('stopwords')

stop = stopwords.words('english')

#deleting stopwords
tweets_info['TWEET'].apply(lambda x: [item for item in x if item not in stop])

#removing emojies from the dataframe
tweets_info.astype(str).apply(lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))


#reading files of positive and negative words
file1 = open('positive-words.txt', 'r')
positive_binglu = file1.readlines()

file2 = open('negative-words.txt', 'r')
negative_binglu = file2.readlines()


import string
from collections import  Counter
import numpy as np
import regex as re



positive_words = []

for words in negative_binglu:
  positive_words.append(re.sub(re.compile('\n'),"", words))
    
negative_words = []

for words in negative_binglu:
  negative_words.append(re.sub(re.compile('\n'),"", words))    


pos = {word:1 for word in positive_words}
neg = {word:-1 for word in negative_words}
pos.update(neg)
sent_dict = pos

def get_sent(tweets_info):
  list_words = tweets_info.split()
  score = 0
  for word in list_words:
    try:
      score+=sent_dict[word]
    except:
      continue
  return score


print()


# sentimentDF = tweets_info.rdd.map(lambda x: (x,get_sent(x))).toDF()
# sentimentDF.show() 


# sentimentOfTweetdf= sentimentOfTweet.toDF().withColumnRenamed("_1","Tweets_Content").withColumnRenamed("_2","Sentiment Score")
# sentimentOfTweetPandas= sentimentOfTweetdf.toPandas()
# sentimentOfTweetPandas.head(10)

