# spark-streaming-with-kafka
This project uses kafka with Spark streaming. 
@erkansirin76's data-generator tool is used to simulate a data stream by constantly streaming the static data. By starting the tool through terminal and directing the stream to test1 by specifying the topic, we can achieve a data stream.
Data is streamed through kafka test1 topic, after reading the stream, it is written to hdfs in parquet format.
