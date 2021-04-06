package testpage

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._

object Test5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val rawJsons = Seq(
      """
        {
          "": [
            {
              "id": "Jacek",
              "name": "Laskowski",
              "count": "1111@japila.pl",
              "addr": "wwwww"
            },
            {
              "id": "Jacek",
              "name": "Laskowski",
              "count": "1111@japila.pl",
              "addr": "wwwww"
            }
          ]
        }
""")
    import org.apache.spark.sql.types._
    import spark.implicits._


    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("avro/test2.avsc")))

//    val schemaExample2 = new StructType()
//      .add($"id".string)
//      .add($"name".string)
//      .add($"count".string)
//      .add($"addr".string)

    val schemaExample2 = new StructType()
      .add("", ArrayType(new StructType()
        .add($"id".string)
        .add($"name".string)
        .add($"count".string)
        .add($"addr".string)
      )
      )
//val schemaExample2 = new StructType()
//  .add("", ArrayType(new StructType()
//    .add("FirstName", StringType)
//    .add("Surname", StringType)
//  )
//  )

    //val dfExample2= spark.sql("""select "{\"\":[{ \"FirstName\":\"Johnny\", \"Surname\":\"Boy\" }, { \"FirstName\":\"Franky\", \"Surname\":\"Man\" }]}" as value""")
    //dfExample2.show()
    val usersDF = spark.createDataset(rawJsons).toDF("value")
    val people = usersDF
      .select(from_json($"value", schemaExample2) as "json")
      .select("json.*") // <-- flatten the struct field
    people.show()


    val structDF = usersDF.select(

        struct(usersDF.columns.map(colname => {col(colname)}):_* )
      ).alias("structAvroCol")
    structDF.printSchema()
    structDF.show()
    val avroDF = usersDF.select(
      to_avro(
        struct(usersDF.columns.map(colname => {col(colname)}):_* )
      ).alias("structAvroCol")
    )
    avroDF.printSchema()
    avroDF.show()


    val user2DF = avroDF.select(from_avro(col("structAvroCol"), jsonFormatSchema).as("user"))
        .select("user.*")

    user2DF.printSchema()
    user2DF.show()
  }
}
