# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
#conf = SparkConf().setAppName("MySparkStreamingApp").setMaster("local[2]")
#sc = SparkContext(conf)
ss = SparkSession.builder.appName("MySparkStreamingApp").getOrCreate()
ssc = StreamingContext(ss.sparkContext, batchDuration=10)

streamRDD = ssc.socketTextStream("127.0.0.1", 2222)
wordCounts = streamRDD.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

wordCounts.pprint(num=100)

ssc.start()
ssc.awaitTermination(timeout=60)
ssc.stop()

