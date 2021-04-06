/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
 *     [<checkpoint-location>]
 *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 *   comma-separated list of host:port.
 *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
 *   'subscribePattern'.
 *   |- <assign> Specific TopicPartitions to consume. Json string
 *   |  {"topicA":[0,1],"topicB":[2,4]}.
 *   |- <subscribe> The topic list to subscribe. A comma-separated list of
 *   |  topics.
 *   |- <subscribePattern> The pattern used to subscribe to topic(s).
 *   |  Java regex string.
 *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
 *   |  specified for Kafka source.
 *   <topics> Different value format depends on the value of 'subscribe-type'.
 *   <checkpoint-location> Directory in which to create checkpoints. If not
 *   provided, defaults to a randomized directory in /tmp.
 *
 * Example:
 *    `$ bin/run-example \
 *      sql.streaming.StructuredKafkaWordCount host1:port1,host2:port2 \
 *      subscribe topic1,topic2`
 */
object StructuredKafkaWordCount {
  def main(args: Array[String]): Unit = {
//    if (args.length < 3) {
//      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
//        "<subscribe-type> <topics> [<checkpoint-location>]")
//      System.exit(1)
//    }

   // val Array(bootstrapServers, subscribeType, topics, _*) = args
    val checkpointLocation = "tmp/temporary"
     // if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

//    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:6667")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(value AS STRING)").as[String]
   import spark.implicits._
  val words = lines.as[String].flatMap(_.split(" "))

   // Generate running word count
   val wordCounts = words.groupBy("value").count()
     .toDF("value","key")
     .selectExpr("CAST(value AS STRING)", "CAST(key AS STRING)")
    print("======wordCounts" + wordCounts)
    // Start running the query that prints the running counts to the console
    import java.util.concurrent.TimeUnit
    import scala.concurrent.duration._
    val query = wordCounts
      .writeStream
      .outputMode("complete")
     .format("kafka")
      .option("kafka.bootstrap.servers", "node1:6667")
      .option("topic", "update2")
      .option("checkpointLocation","uodate2")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .start()

    val query2 = wordCounts
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:6667")
      .option("topic", "orders")
      .option("checkpointLocation","uodate")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .start()

    query.awaitTermination()
    query2.awaitTermination()
  }

}
// scalastyle:on println
