import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("Read from kafka").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0").getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

iris_schema ="row_id int, SepalLengthCm float, SepalWidthCm float, PetalLengthCm float, PetalWidthCm float, Species string, time timestamp"

lines = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","test1")
    .option("startingOffsets","earliest")
    .option("failOnDataLoss","false")
    .load()
)
# Operasyon


lines2 = lines.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","topic","partition","offset","timestamp","timestampType")



#write file stream to a sink
streamingQuery = (
    lines2.writeStream
    .format("parquet")
    .outputMode("append")
    .trigger(processingTime = "2 seconds")
    .option("path", "hdfs://localhost:9000/user/train/kafka-deneme") #hata alırsan bu folder başlatmadan önce varsa folderı sil
    .option("checkpointLocation", "file:///home/train/checkpoint-new-from-kafka") #hata alırsan bu folder başlatmadan önce varsa folderı sil
    .start()
                  )



streamingQuery.awaitTermination(100)
