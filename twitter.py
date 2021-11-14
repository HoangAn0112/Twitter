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
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud

##################################################3
## Load twitter data from mongodb
client = MongoClient(host="127.0.0.1:27017")
db = client["mydb"] # choix de la DB
# print(client)
# print(db)
# print("Collections contenues dans la base mydb :",
#       db.list_collection_names())


##  Load sentiments file 
sentiment = pd.read_csv(r'data/twitter_sentiment_data.csv')
    
    # extract tweet ID for hydration from Twitter
sentiment['tweetid'].to_csv(r'data/ID_py.txt', 
                            header=None, index=None, sep = ' ', mode= 'a')

## load collection twitter
tw = db.twitter

## Count number of tweet
number = tw.count()       ## only hydrated successfully 27347 tweets (62%)

# # How many tweet for each language
language = tw.aggregate([
            { "$group": {
                "_id": '$lang',
                "count": {"$sum": 1}
                }},
            ])     
# print(list(language))   # still has the same 18433 tweets in english 
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
    tw.update_one(
        {"_id": tweet[i]["_id"]},
        { "$set": {"sentiment": int(b[0]['sentiment']) }}
        )

## check if sentiment is updated by loading ramdom tweet
check = list(tw.find())
random = 23
print("Sentiment value of tweet", random +1 ,"= {}\n".format(check[random]["sentiment"]))


print("==========")
print("TASK 2: counting how many tweets for each sentiment group")
sentiment = tw.aggregate([
            { "$group": {
                "_id": '$sentiment',
                "count": {"$sum": 1}
                }},
            {"$sort": {
                "count": -1         # ranking from biggest number of tweets to smallest
                }}
            ])     
# print(list(sentiment))              # -1: don't believe in climate change (1742)
                                    #  0: neutre opinion (4425)
                                    #  1: believe in climate change (14840)
                                    #  2: report news about climate change (6340)
# for level in sentiment:
#     print(level)

print("==========\nBarplot about sentiment")

df = pd.DataFrame(list(sentiment))
print(df)
df["_id"][df["_id"]==-1] = "don't believe in climate change"
df["_id"][df["_id"]==0] = "neutre opinion"
df["_id"][df["_id"]==1] = "believe in climate change"
df["_id"][df["_id"]==2] = "report news about climate change"

# # add percentage column
# df["percent"] = round(df["count"]/sum(df["count"]), 4)*100

ax = df.plot.barh(y = 'count', x = '_id', legend = False, color = "#1DA1F3")
ax.set_xlabel("Number of tweets")
ax.set_ylabel("")
ax.figure.savefig('barplot_sentiment.png',
                  dpi = 200, bbox_inches = "tight")


print("==========")
print("TASK 3: Most popular # by sentiment")

# hashtags = tw.find({"entities.hashtags": {"$ne": []}},
#                       {"entities.hashtags.text": 1})
# hashtags = tw.find({"entities.hashtags": {"$exists": True, "$not": {"$size": 0}}},  # tweets containing hashtags
#                       {"sentiment":1, "entities.hashtags.text": 1})

top_hashtags = tw.aggregate([
    {"$unwind": "$entities.hashtags"},
    {"$group" : {"_id":{"sentiment":"$sentiment", "hashtag": "$entities.hashtags.text"},
                 "Nb#":{"$sum": 1}}},
    {"$sort": {"Nb#": -1}}
])

# building top 3 of hashtags by sentiment
hashtag_list = []
top_dict = {-1: 0, 0: 0, 1: 0, 2: 0}
for doc in top_hashtags:
    # pp.pprint(doc)
    if top_dict[doc["_id"]["sentiment"]] < 5:
        hashtag_list.append({"Frequency": doc["Nb#"],
                            "Hashtag": doc["_id"]["hashtag"],
                            "Sentiment": doc["_id"]["sentiment"]})   
        top_dict[doc["_id"]["sentiment"]] += 1
# pp.pprint(hashtag_list)

for elem in hashtag_list:
    if elem["Sentiment"] == -1:
        elem["Opinion"] = "don't believe in climate change"
    elif elem["Sentiment"] == 0:
        elem["Opinion"] = "neutre opinion"
    elif elem["Sentiment"] == 1:
        elem["Opinion"] = "believe in climate change"
    else:
        elem["Opinion"] = "report news about climate change"
  
df = pd.DataFrame(hashtag_list)
print(df)

def 

for key in top_dict.keys():
    words = df["Hashtag"].values
    cloud = WordCloud(width=800, height=400, background_color="white", max_words=50, min_font_size=10).generate(str(words))
    plt.imshow(cloud)
    plt.axis("off")
    plt.title(key)
    plt.tight_layout(pad=0)
    plt.show()



