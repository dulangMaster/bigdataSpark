package testpage

import org.apache.spark.sql.SparkSession

object Spark2 {
  import org.apache.spark.sql.avro._

  // Read a Kafka topic "t", assuming the key and value are already
  // registered in Schema Registry as subjects "t-key" and "t-value" of type
  // string and int. The binary key and value columns are turned into string
  // and int type with Avro and Schema Registry. The schema of the resulting DataFrame
  // is: <key: string, value: int>.
  val spark = SparkSession.builder.appName("StructuredKafkaWordCount")
    .master("local[*]")
    .getOrCreate
  val schemaRegistryAddr = "https://www.iteblog.com"

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "node1:6667")
    .option("subscribe", "t")
    .load()
//    .select(
//      from_avro("key", "t-key", schemaRegistryAddr).as("key"),
//      from_avro("value", "t-value", schemaRegistryAddr).as("value"))

  // Given that key and value columns are registered in Schema Registry, convert
  // structured data of key and value columns to Avro binary data by reading the schema
  // info from the Schema Registry. The converted data is saved to Kafka as a Kafka topic "t".

}
