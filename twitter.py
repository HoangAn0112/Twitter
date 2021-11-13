# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd    
#jsonObj = pd.read_json(path_or_buf= "/Users/Admin/Documents/M2/Big data/Project/ID.jsonl", lines=True)
from pymongo import MongoClient
import pprint as pp # library use to make output more pretty
import json
import pandas as pd  

##################################################3
## Load twitter data from mongodb
client = MongoClient(host="127.0.0.1:27017")
db = client["mydb"] # choix de la DB
print(client)
print(db)
print("Collections contenues dans la base mydb :",
      db.list_collection_names())


##  Load sentiments file 
sentiment = pd.read_csv(r'/Users/Admin/Documents/M2/Big data/Project/twitter_sentiment_data.csv')
    
    # extract tweet ID for hydration from Twitter
sentiment['tweetid'].to_csv(r'/Users/Admin/Documents/M2/Big data/Project/ID_py.txt', 
                            header=None, index=None, sep = ' ', mode= 'a')

## load database twitter
tw = db.twitter

## Count number of tweet
number = tw.count()       ## only hydrated successfully 27347 tweets (62%)

# # How many tweet for each language
language = db.tweets.aggregate([
            { "$group": {
                "_id": '$lang',
                "count": {"$sum": 1}
                }},
            ])     
print(list(language))   # still has the same 18433 tweets in english 
                        # (compare to previous database with R), 
                        # but I don't know how to print the pretty way

## Tweet in english 
query = {"lang": "en"}
   
tw.find_one(query)           ## first tweet 

en = tw.find(query)         ## all tweets in english

re =  list(tw.find(query))
print(re[0]["full_text"])           # print content of first tweet only
print(re[0]["user"]["id_str"])      # print id string in "user" subdocument

    # print more tweet: example 3 first tweet
for i in range(0,3):
    print("Tweet", i+1 ,"content = {}\n".format(re[i]["full_text"]))
        
#####################################################
## TASK 1: add new field "sentiment" to every document in collection(twitter),
## with condition match ID in twitter_sentiment_data.csv

tweet = list(tw.find())
for i in range(0,number):
    a = int(tweet[i]["id_str"]),
    b = sentiment.loc[(sentiment['tweetid']==a)],
    sen = b[0],
    tw.update_one(
        {"_id": tweet[i]["_id"]},
        { "$set": {"sentiment": sen }}
        )


    







