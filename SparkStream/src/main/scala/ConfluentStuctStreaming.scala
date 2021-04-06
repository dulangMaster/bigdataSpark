

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.streaming.Trigger
import untis.{KafkaSink, KafkaSink2}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructType, _}
import org.apache.avro.SchemaBuilder

object ConfluentStuctStreaming {

  import org.apache.spark.sql.types._

  //val  jsonFormatSchema = addressesSchema.json.toString
  //val jsonFormatSchema = "{\"type\":\"record\",\"name\":\"test_confulent\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"count\",\"type\":[\"null\",\"int\"]},{\"name\":\"modified\",\"type\":{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\"}}],\"connect.name\":\"test_confulent\"}"
  val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("avro/test.avsc")))

  import org.apache.spark.sql.types.StructField
  import org.apache.spark.sql.types.StructType

  val schema: StructType = DataTypes.createStructType(Array[StructField](
    DataTypes.createStructField("id", DataTypes.StringType, false),
    DataTypes.createStructField("name", DataTypes.StringType, false),
    DataTypes.createStructField("count", DataTypes.IntegerType, false),
    DataTypes.createStructField("modified", DataTypes.LongType, false)))

  def main(args: Array[String]): Unit = { // Creating local sparkSession here...
    val spark = SparkSession.builder.appName("StructuredKafkaWordCount")
      .master("local[*]")
      .getOrCreate

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:6667")
      .option("subscribe", "mysql-test-test_confulent")
      .load
//      .selectExpr("CAST(value AS STRING) as message")
//      .select(from_json(col("message"), schema).as("json"))
//      .select("json.*")
//    implicit val statisticsRecordEncoder = Encoders.product[StatisticsRecord]
//    val myDeserializerUDF = udf { bytes => deserialize("hello", bytes) }
//    df.select(myDeserializerUDF($"value") as "value_des")
    val kafkaSink = new KafkaSink2("weblogs2", "node1:6667")
    val query = df
      .writeStream
      .foreach(kafkaSink)
      .outputMode("update")
      .option("checkpointLocation", "update")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .start()
    query.awaitTermination()
  }
}