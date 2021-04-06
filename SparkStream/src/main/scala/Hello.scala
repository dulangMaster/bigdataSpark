import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import untis.JDBCSink

object Hello {
  case class testCount(value: String, count: String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .master("local[*]")
      .getOrCreate()
    val df = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 1002)
      .load()

    import spark.implicits._
    val lines = df.selectExpr("CAST(value AS STRING)").as[String]
    val weblog = lines.map(_.split(" "))
      .map(x => testCount(x(0), x(1)))

    val titleCount = weblog.groupBy("count").count().toDF("value", "count")

    print("wordCounts++++++" + titleCount)

    val url="jdbc:mysql://172.20.17.61:3306/spring"
    val user="root"
    val pwd="123456"
    val writer = new JDBCSink(url, user, pwd)

    val query=titleCount
      .writeStream
      .foreach(writer)
      .outputMode("complete")
      .trigger( Trigger.ProcessingTime(1,TimeUnit.SECONDS))
      .start()
    query.awaitTermination()
  }
}
