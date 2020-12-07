
## Step 1
A screenshot of the kafka-consumer-console output <br>
![kafka-consumer-console](https://raw.githubusercontent.com/wilfreditoq2/udacity_streaming_trainee/main/SF_Crime_Statistics_with_spark_streaming/screenshots/Kafka_Consumer_Console.png)

Kafka consumer server
![kafka-consumer-server](https://raw.githubusercontent.com/wilfreditoq2/udacity_streaming_trainee/main/SF_Crime_Statistics_with_spark_streaming/screenshots/kafka_consumer_server.png)

## Step 2

A screenshot of the progress reporter after executing a Spark job
![progress-reporter-spart](https://raw.githubusercontent.com/wilfreditoq2/udacity_streaming_trainee/main/SF_Crime_Statistics_with_spark_streaming/screenshots/spark_progress_exec_1.png)

![progress-reporter-spart](https://raw.githubusercontent.com/wilfreditoq2/udacity_streaming_trainee/main/SF_Crime_Statistics_with_spark_streaming/screenshots/spark_progress_exec_2.png)

A screenshot of the Spark Streaming UI as the streaming continues
![spark-streaming-UI](https://raw.githubusercontent.com/wilfreditoq2/udacity_streaming_trainee/main/SF_Crime_Statistics_with_spark_streaming/screenshots/spark_streaming_UI.png)

## Step 3

### 1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

In theory the bigger is the "maxOffsetPerTrigger", the the more records Spark should process per batch. In our case as the data set is not that big, we should not be big difference if we increase by 100. 

### 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

In a cluster, to allow spark to use the available ressources, I could think of *spark.default.parallelism* to consume the records faster. Other options are also *maxOffsetPerTrigger*  and *maxRatePerPartition*, that allows spark to increase the number of records to read per batch and/or seconds. In this case *200* for both provided a fast and stable streaming.
