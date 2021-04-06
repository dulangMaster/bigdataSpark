package testpage

object test3 {
  import org.apache.spark.sql.{ Row, SparkSession, Column, functions }
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.avro._
  import java.nio.file.{Files, Paths}

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val jsonDS = spark.createDataset(
      """{"name": "Alyssa", "favorite_color": "blue"} """ ::
        """{"name": "Ben", "favorite_color": "red" } """ :: Nil)
    val usersDF = spark.read.json(jsonDS)
    usersDF.show()
    usersDF.printSchema()

    val structDF = usersDF.select(
      struct(usersDF.columns.map(colname => {col(colname)}):_* ).alias("structCol")
    )
    structDF.printSchema()
    structDF.show()

    val avroDF = usersDF.select(
      to_avro(
        struct(usersDF.columns.map(colname => {col(colname)}):_* )
      ).alias("structAvroCol")
    )
    avroDF.printSchema()
    avroDF.show()

    val jsonFormatSchema = """
      {
    "type":"record",
    "name":"User",
    "fields":[
      {
        "name":"name",
        "type":"string"
      },
      {
        "name":"favorite_color",
        "type":[
          "string",
          "null"
        ]
      }
    ]
  }
      """
    val user2DF = avroDF.select(from_avro(col("structAvroCol"), jsonFormatSchema).as("user"))

    user2DF.printSchema()
//    user2DF.show()

    spark.close()

  }

}
