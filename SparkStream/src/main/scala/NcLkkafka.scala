import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import untis.JDBCSink

object NcLkkafka {
  case class testCount(value:String,count:String)
  def main(args: Array[String]): Unit = {
    val spark =  SparkSession.builder().master("local[2]")
      .getOrCreate()


    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 1002)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val url="jdbc:mysql://172.20.17.61:3306/spring"
    val user="root"
    val pwd="123456"
    val writer = new JDBCSink(url, user, pwd)

    val query=wordCounts
      .writeStream
      .foreach(writer)
      .outputMode("complete")
      .trigger( Trigger.ProcessingTime(1,TimeUnit.SECONDS))
      .start()

    query.awaitTermination()
  }
}
