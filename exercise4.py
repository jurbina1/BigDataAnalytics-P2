from pyspark.sql import SparkSession
import csv, re, datetime, time
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
    stop = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he’d", "he’ll", "he’s", "her", "here", "here’s", "hers", "herself", "him", "himself", "his", "how", "how’s", "I", "I’d", "I’ll", "I’m", "I’ve", "if", "in", "into", "is", "it", "it’s", "its", "itself", "let’s", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she’d", "she’ll", "she’s", "should", "so", "some", "such", "than", "that", "that’s", "the", "their", "theirs", "them", "themselves", "then", "there", "there’s", "these", "they", "they’d", "they’ll", "they’re", "they’ve", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we’d", "we’ll", "we’re", "we’ve", "were", "what", "what’s", "when", "when’s", "where", "where’s", "which", "while", "who", "who’s", "whom", "why", "why’s", "with", "would", "you", "you’d", "you’ll", "you’re", "you’ve", "your", "yours", "yourself", "yourselves"]
    oauth = OAuth(access_token, access_secret, consumer_key, consumer_secret)
    twitter_stream = TwitterStream(auth=oauth)
    spark = SparkSession.builder.getOrCreate()

    # Start Files
    with open('hashtags.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['Hashtag', 'Year', 'Month', 'Day', 'Hour'])
    with open('users.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['User', 'Year', 'Month', 'Day', 'Hour'])
    with open('words.csv', 'w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['Keyword', 'Year', 'Month', 'Day', 'Hour'])

    tweet_count = 50

    tweet_time = 1 #Hours of execution
    for i in range (0,tweet_time):
        end = time.time() + (60 * 60)
        iterator = twitter_stream.statuses.sample()
        for tweet in iterator:
            try:

                if tweet['lang'] == 'en':
                    now = datetime.datetime.now()
                    user = tweet['user']['screen_name']
                    with open('users.csv', 'a') as csvfile:
                        filewriter = csv.writer(csvfile, delimiter=',',
                                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
                        filewriter.writerow([user, now.year, now.month, now.day, now.hour])

                    text = tweet['extended_tweet']['full_text']
                    text = re.sub(r'[^A-Za-z0-9\s]', "", text)
                    words = text.split()
                    for word in words:
                        if not(word.lower() in stop):
                            with open('words.csv', 'a') as csvfile:
                                filewriter = csv.writer(csvfile, delimiter=',',
                                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                                filewriter.writerow([word.lower(), now.year, now.month, now.day, now.hour])

                    if tweet['extended_tweet']['entities']['hashtags']:
                        tweet_count -= 1

                        try:
                            htgs = []
                            for ht in tweet['extended_tweet']['entities']['hashtags']:
                                htgs.append([ht['text'], now.year, now.month, now.day, now.hour])

                            with open('hashtags.csv', 'a') as csvfile:
                                filewriter = csv.writer(csvfile, delimiter=',',
                                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
                                for ht in htgs:
                                    filewriter.writerow([ht[0], ht[1], ht[2], ht[3], ht[4]])
                        except:
                            pass
                    elif tweet['lang'] == 'en' and tweet['entities']['hashtags']:
                        tweet_count -= 1

                        try:
                            htgs = []
                            for ht in tweet['entities']['hashtags']:
                                htgs.append([ht['text'], now.year, now.month, now.day, now.hour])

                            with open('hashtags.csv', 'a') as csvfile:
                                filewriter = csv.writer(csvfile, delimiter=',',
                                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
                                for ht in htgs:
                                    filewriter.writerow([ht[0], ht[1], ht[2], ht[3], ht[4]])
                        except:
                            pass

                if time.time() >= end:
                    break
            except:
                pass
        df = spark.read.csv("hashtags.csv", header=True,
                            inferSchema=True)
        df.createOrReplaceTempView("hashtags")
        sqlResult = spark.sql("select Hashtag, count(*) as Total from hashtags group by Hashtag order by Total desc limit 10")
        jsonResult = sqlResult.toJSON().collect()
        print(jsonResult)

        df = spark.read.csv("users.csv", header=True,
                            inferSchema=True)
        df.createOrReplaceTempView("users")
        sqlResult = spark.sql(
            "select User, count(*) as Total from users group by User order by Total desc limit 10")
        jsonResult = sqlResult.toJSON().collect()
        print(jsonResult)

        df = spark.read.csv("words.csv", header=True,
                            inferSchema=True)
        df.createOrReplaceTempView("words")
        sqlResult = spark.sql(
            "select Keyword, count(*) as Total from words group by Keyword order by Total desc limit 10")
        jsonResult = sqlResult.toJSON().collect()
        print(jsonResult)

if __name__ == "__main__":
    print("Starting to read tweets")
    credentials = read_credentials()
    read_tweets(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'],
                credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
