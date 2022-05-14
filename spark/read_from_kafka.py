spark = SparkSession \
          .builder \
          .appName("APP") \
          .getOrCreate()

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "host.docker.internal:9094") \
      .option("subscribe", "sparktest") \
      .option("startingOffsets", "earliest") \
      .load()
      

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .start()

query.awaitTermination()