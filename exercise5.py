from pyspark.sql import SparkSession
import csv, datetime, time, subprocess, re
try:
    import json
except ImportError:
    import simplejson as json

from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

def read_credentials():
    file_name = "credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load credentials.json")
        return None



def read_tweets(access_token, access_secret, consumer_key, consumer_secret):
    oauth = OAuth(access_token, access_secret, consumer_key, consumer_secret)
    twitter_stream = TwitterStream(auth=oauth)
    spark = SparkSession.builder.getOrCreate()

    # Start File
    with open('keywords.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['Keyword', 'Year', 'Month', 'Day', 'Hour'])


    tweet_time = 4 #Hours of execution
    for i in range (0,tweet_time):
        end = time.time() + (60 * 60)
        iterator = twitter_stream.statuses.sample()
        for tweet in iterator:
            try:
                if tweet['lang'] == 'en':
                    hasKeyword = False
                    now = datetime.datetime.now()
                    text = tweet['extended_tweet']['full_text']
                    words = ["trump", "flu", "zika", "diarrhea", "headache", "measles", "ebola"]
                    for word in words:
                        if word in text.lower():
                            hasKeyword = True
                            with open('keywords.csv', 'a') as csvfile:
                                filewriter = csv.writer(csvfile, delimiter=',',
                                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                                filewriter.writerow([word, now.year, now.month, now.day, now.hour])
                    if hasKeyword:
                        temp = re.sub(r'[^A-Za-z0-9\s]', "", tweet['extended_tweet']['full_text'])
                        temp = json.dumps({"text": temp})
                        #print(temp)
                        subprocess.Popen('echo \'{0}\' | hadoop fs -appendToFile - /app/result.json'.format(temp), shell=True)
                if time.time() >= end:
                    break
            except:
                pass
        df = spark.read.csv("keywords.csv", header=True,
                            inferSchema=True)
        df.createOrReplaceTempView("keywords")
        sqlResult = spark.sql(
            "select Keyword as label, count(*) as value from keywords group by Keyword order by value desc")
        sqlResult.show()
        print(sqlResult.toJSON().collect())



if __name__ == "__main__":
    print("Starting to read tweets")
    credentials = read_credentials()
    read_tweets(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'],
                credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
