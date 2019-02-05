from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    flatlist_counts = [item for sublist in counts for item in sublist]
    positives = list(map(lambda x: x[1], list (filter(lambda z:z[0] == 'Positive',flatlist_counts))))
    negatives = list(map(lambda x: x[1], list (filter(lambda z:z[0] == 'Negetive',flatlist_counts))))
    plt.plot(positives, c ='b',linestyle = '-',marker = 'o',label = 'positives')
    plt.plot(negatives, c ='g',linestyle = '-',marker = 'o',label = 'negatives')
    plt.legend(loc = 'upper left')
    plt.ylabel('Word Count')
    plt.ylabel('Time Step')
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    word_list = set(line.strip() for line in open(filename,"r"))
    return word_list

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    #tweets.pprint()
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    words = tweets.flatMap(lambda line: line.split(" "))
    words = words.filter(lambda x: x in pwords or x in nwords)
    pos_neg_words = words.map(lambda x: ("Positive",1) if x in pwords else ("Negetive",1))
    #pos_neg_words.pprint()
    
    count_pos_neg_words = pos_neg_words.reduceByKey(lambda x,y: x+y)
    #count_pos_neg_words.pprint()

    runningcount = count_pos_neg_words.updateStateByKey(updateFunction)
    runningcount.pprint()
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    count_pos_neg_words.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()