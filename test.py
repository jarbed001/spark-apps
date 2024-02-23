from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("testApp").getOrCreate()

df = ss.read.json("people.json")

df.show()

ss.stop()




