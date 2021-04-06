import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import untis.KafkaSink

object Kafkagroupby {

  case class Weblog(searchname: String,
                    userid: String,
                    retorder: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("streaming")
      .getOrCreate()
    val df = spark
      .readStream
      .format("kafka")
//      .option("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
//      .option("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      .option("kafka.bootstrap.servers", "node1:6667")
      .option("subscribe", "weblogs")
      .load()

    import spark.implicits._
    val lines = df.selectExpr("CAST(value AS STRING)").as[String]
    val weblog = lines.map(_.split(","))
      .map(x => Weblog(x(0), x(1), x(2)))
    val titleCount = weblog
      .groupBy("searchname").count().toDF("titleName", "count")


    val kafkaSink = new KafkaSink("weblogs2", "node1:6667")
    val query = titleCount.writeStream
      .foreach(kafkaSink)
      .outputMode("update")
      .option("checkpointLocation", "uodate")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .start()
    query.awaitTermination()
  }
}
