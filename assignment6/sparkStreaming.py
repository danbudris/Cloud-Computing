
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add
import argparse

# set up a main function, so we can take advantage of argument inputs
def main(socketPort, saveAsFile, outPath, logLevel):

    def aggregate_count(new_values, total_sum):
        return sum(new_values) + (total_sum or 0)

    # create spark configuration
    conf = SparkConf()
    conf.setAppName("CongressTwitterStream")

    # create spark context with the above configuration
    sc = SparkContext(conf=conf)

    # logLevel can be "FATAL", "INFO", "ERROR"
    sc.setLogLevel(logLevel)

    # create the Streaming Context from the above spark context with interval size 2 seconds
    ssc = StreamingContext(sc, 2)

    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("checkpoint_TwitterApp")

    # read data from port 9009
    dataStream = ssc.socketTextStream("localhost", socketPort)

    # split each tweet into words
    words = dataStream.window(120, 6).flatMap(lambda line: line.split(" "))

    # Get the hashtags contained in tweets that contain references to the U.S. Senate twitter accounts
    hashtags = words.filter(lambda w: ('#' in w))\
                    .filter(lambda w: not (('http' in w) or ('\\u' in w) or (len(w)==1) or ('/RT/' in w)))\
                    .map(lambda x: (x.lower(), 1))\
                    .reduceByKey(add)

    # adding the count of each hashtag to its last count
    tags_totals = hashtags.updateStateByKey(aggregate_count)\
                        .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    # get the reply values for a given tweet about a U.S. Senate twitter account, and start tracking it -- we want to know how is replying and tweeting about congress
    targets = words.filter(lambda w: ('@' in w))\
                    .filter(lambda w: not (('http' in w) or ('\\u' in w) or (len(w)==1) or ('/RT/' in w) ))\
                    .map(lambda x: (x.lower(), 1))\
                    .reduceByKey(add)

    targets_total = targets.updateStateByKey(aggregate_count)\
                        .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    # get the reply value of a given tweet, so we can see who each tweet is in reply to, and thus see who is driving the converations about Congress
    replies = words.filter(lambda w: ('/RT/'in w))\
                .filter(lambda w: not (('http' in w) or ('\\u' in w) or (len(w)==1) ))\
                .map(lambda x: (x.lower(), 1))\
                .reduceByKey(add)

    replies_total = replies.updateStateByKey(aggregate_count)\
                        .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    # Print out the top twenty tags
    tags_totals.pprint(20)

    # Print out the top twenty @'s for congress
    targets_total.pprint(20)

    # Print out the reply strings for each tweet
    replies_total.pprint(20)

    # if text output is desired, save this as a text file; this gets huge FAST; defaults to no
    if saveAsFile:
        replies_total.saveAsTextFiles(outPath)

    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()

if __name__ == "__main__":
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9009)
    parser.add_argument("--save", type=bool, default=False)
    parser.add_argument("--outPath", default ="./")
    parser.add_argument("--logLevel", default="FATAL")
    args = parser.parse_args()

    # run the main function with the given command line arguments
    main(args.port, args.save, args.outPath, args.logLevel)