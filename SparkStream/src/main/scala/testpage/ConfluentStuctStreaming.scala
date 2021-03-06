package testpage

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import ConfluentStuctStreaming.jsonFormatSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import untis.KafkaSink2
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._


object ConfluentStuctStreaming {

  import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
  import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
  import org.apache.avro.Schema
  import org.apache.avro.generic.GenericRecord
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.avro._

  //val  jsonFormatSchema = addressesSchema.json.toString
  //val jsonFormatSchema = "{\"type\":\"record\",\"name\":\"test_confulent\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"count\",\"type\":[\"null\",\"int\"]},{\"name\":\"modified\",\"type\":{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\"}}],\"connect.name\":\"test_confulent\"}"
  val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("avro/test.avsc")))


  private var schemaRegistryClient: SchemaRegistryClient = _
  private var kafkaAvroDeserializer: AvroDeserializer = _

  def getTopicSchema(topic: String) = {
    schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema
  }

  def avroSchemaToSparkSchema(avroSchema: String) = {

    SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("kafka-structured").set("spark.testing.memory", "2147480000")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val bootstrapServers = "node1:6667"
    val topic = "mysql-test-test_confulent2"
    val schemaRegistryUrl = "http://node2:8081"
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    consumeAvro(spark, bootstrapServers, topic, schemaRegistryUrl)
    print(">>>>>>>>" + getTopicSchema(topic))
    spark.stop()
  }

  def consumeAvro(spark: SparkSession, bootstrapServers: String, topic: String, schemaRegistryUrl: String): Unit = {
    import spark.implicits._

    schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)

    kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes)
    )

    val rawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("group.id", "1")
      .load()
    val rawSchema2 = getTopicSchema(topic)
    print(">>>>>" + rawSchema2)
    import org.apache.spark.sql.functions._
    val jsonDf = rawDf.select(callUDF("deserialize", 'value).as("value"))
    val dfValueSchema = {
      val rawSchema = getTopicSchema(topic)
      avroSchemaToSparkSchema(rawSchema)
    }
    val parsedDf = jsonDf.select(from_json('value, dfValueSchema.dataType).alias("value")
    ).select($"value.*")
    parsedDf.createTempView("test")
    val output = spark.sql("select id,name,sum(count) from test group by id,name")

    output.writeStream
      .format("console")
      .outputMode("complete")
      //.outputMode("append ")
      .start()
      .awaitTermination()
  }


  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val value = super.deserialize(bytes)
      value match {
        case str: String =>
          str
        case _ =>
          val genericRecord = value.asInstanceOf[GenericRecord]
          if (genericRecord == null) {
            // ??????????????????
            null
          } else {
            genericRecord.toString

          }
      }
    }
  }

}