# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#jsonObj = pd.read_json(path_or_buf= "/Users/Admin/Documents/M2/Big data/Project/ID.jsonl", lines=True)
from pymongo import MongoClient
import pprint as pp # library use to make output more pretty
import json
import pandas as pd  

###################################################
import os
os.environ['JAVA_HOME'] = 'C:/Java/jre1.8.0_311/'

##################################################
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
language = tw.aggregate([
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
    tw.update_one(
        {"_id": tweet[i]["_id"]},
        { "$set": {"sentiment": int(b[0]['sentiment']) }}
        )

## check if sentiment is updated by loading ramdom tweet
check = list(tw.find())
random = 23
print("Sentiment value of tweet", random +1 ,"= {}\n".format(check[random]["sentiment"]))


## TASK 2: counting how many tweets for each sentiment group
sentiment = tw.aggregate([
            { "$group": {
                "_id": '$sentiment',
                "count": {"$sum": 1}
                }},
            {"$sort": {
                "count": -1         # ranking from biggest number of tweets to smallest
                }}
            ])     
#print(list(sentiment))             # -1: don't believe in climate change (1742)
                                    #  0: neutre opinion (4425)
                                    #  1: believe in climate change (14840)
                                    #  2: report news about climate change (6340)
#for level in sentiment:
#   print(level)

# Rename each sentiment class
print("==========\nBarplot of sentiments")

df = pd.DataFrame(list(sentiment))
print(df)
df["_id"][df["_id"]==-1] = "don't believe in climate change"
df["_id"][df["_id"]==0] = "neutre opinion"
df["_id"][df["_id"]==1] = "believe in climate change"
df["_id"][df["_id"]==2] = "report news about climate change"

#Draw plot
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

def for key in top_dict.keys():
    words = df["Hashtag"].values
    cloud = WordCloud(width=800, height=400, background_color="white", max_words=50, min_font_size=10).generate(str(words))
    plt.imshow(cloud)
    plt.axis("off")
    plt.title(key)
    plt.tight_layout(pad=0)
    plt.show()


###########################
## INITIALIZING A SPARK CONTEXT
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType,StringType)
from pyspark.sql.functions import desc
import re

spark = SparkSession \
 .builder \
 .master('local[*]')\
 .appName("Twitter") \
 .config("spark.executor.memory","1g") \
 .config("spark.driver.maxResultSize","0") \
 .getOrCreate()
 
#sc = spark.sparkContext
  
#my_spark = SparkSession \
#        .builder \
#        .appName("Twitter") \
#        .config("spark.mongodb.input.uri","mongodb://127.0.0.1:27017/mydb.twitter")\
#        .config("spark.mongodb.output.uri","mongodb://127.0.0.1:27017/mydb.twitter")\
#        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12-3.0.1') \
#        .getOrCreate()
#df = my_spark.read.format("mydb.twitter").load() 
#df = my_spark.read.format()  
    
#twitter = sc.textFile("/Users/Admin/Documents/M2/Big data/Project/ID_py.csv")
#df = spark.read.json("/Users/Admin/Documents/M2/Big data/Project/ID_py.jsonl",multiLine = False)
#twitter = spark.read.csv("/Users/Admin/Documents/M2/Big data/Project/twitter_sentiment_data.csv", header = True)

df = spark.createDataFrame(sentiment)

##Inspect data
df.count()      #43943
df.columns
df.printSchema()
df.describe().show()

##Show some tweets
df.select('message').limit(3).toPandas()

##Drop duplicate and na rows
df = df.dropDuplicates()
df = df.na.drop()
df.count()         #43943

## Group by sentiments
df.groupby("sentiment").count().show()

## Some word counting
#rdd = df.rdd       #turn dataframe into rdd]
#climate = rdd.filter(lambda x: "climate" in x['message'])
#print("Climate", climate.count()) 


##Tweet cleaning
# use PySparks build in tokenizer to tokenize tweets
tokenizer = Tokenizer(inputCol  = "message",
                      outputCol = "token")
tweet = tokenizer.transform(df)
tweet.repartition(20).limit(2).select('message','token').toPandas()

# Remove stop words
remover = StopWordsRemover(inputCol='token', 
                           outputCol='token_nostp')
tweet_remove = remover.transform(tweet)
tweet_remove.repartition(20).limit(2).select('token','token_nostp').toPandas()

# Remove retweet
tweet2 = tweet_remove.filter(tweet_remove.token[0]!="rt")         # retweet has rt as the first token
tweet2.count()          #left 18881 tweets

# Cleaning hashtag, web link, tag in token
## Function to remove hashtag, web link, tag in token
def remove(token: list) -> list:
    """
    Removes hashtags, call outs and web addresses from tokens.
    """
    expr = '(@[A-Za-z0-a9_]+)|'+\
            '(#[A-Za-z0-9_]+)|'+\
            '(https?://[^\s<>"]+|www\.[^\s<>"]+)'   
        
    regex   = re.compile(expr)

    cleaned = [t for t in token if not(regex.search(t)) if len(t) > 0]

    return list(filter(None, cleaned))

## Drap function 'remove' to spark by UDF
remove = F.udf(remove, ArrayType(StringType()))

## Pass function through data.frame
tweet3 = tweet2.withColumn("tokens_clean", remove(tweet2["token_nostp"]))
tweet3.repartition(500).limit(3).select('token_nostp','tokens_clean').toPandas()


# Remove tweets where the tokens array is empty, i.e. where it was just
# hashtag, web adress etc.
tweet = tweet3.where(F.size(F.col("tokens_clean")) > 0)
tweet.limit(2).toPandas()
tweet.count()        #18853 tweets left

# Save final dataset
tweet.repartition(20).select("sentiment","tweetid","tokens_clean") \
    .write \
    .save("twitter_cleared.json",format = "json")

# Stop spark session
spark.stop

############## MAP REDUCE #########################
conf = SparkConf() \
    .setMaster("local[*]") \
    .setAppName("Twitter") \
    .set("spark.executor.memory","1g")   
sc = SparkContext.getOrCreate(conf=conf)


## Seperate by sentiment class, count words and sort by decending 
## and take the 10 biggest word frequencies

results = []
sen = ("-1","0","1","2")
for i in sen:
    RDD = tweet.filter(tweet['sentiment']==i)
    RDD = RDD.select("tokens_clean").rdd
    RDD = RDD.flatMap(lambda x: x[0]) \
              .map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y)
    RDD = RDD.repartition(20).toDF()
    word = RDD.orderBy(desc(RDD[1])).take(10)     #list type, could use print()
    results.append(word)
    
#save file
file = json.dumps(results)
#save file
#rdd.repartition(1000).saveAsTextFile("file:///C:/temp/WordCountTotal")

## Stop sparkContext
sc.stop()
















    







