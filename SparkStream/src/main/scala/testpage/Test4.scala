package testpage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, from_json}

object Test4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val rawJsons = Seq("""
  {
    "firstName" : "Jacek",
    "lastName" : "Laskowski",
    "email" : "jacek@japila.pl",
    "addresses" : [
      {
        "city" : "Warsaw",
        "state" : "N/A",
        "zip" : "02-791"
      }
    ]
  }
""")
    import org.apache.spark.sql.types._
    import spark.implicits._
    val addressesSchema = new StructType()
      .add($"city".string)
      .add($"state".string)
      .add($"zip".string)
    val schema = new StructType()
      .add($"firstName".string)
      .add($"lastName".string)
      .add($"email".string)
      .add($"addresses".array(addressesSchema))
    val schemaAsJson = schema.json
    import org.apache.spark.sql.types.DataType
    val dataType = DataType.fromJson(schemaAsJson)

    val df = spark.createDataset(rawJsons).toDF
    val people = df
      .select(from_json($"value", dataType) as "json")
      .select("json.addresses.city") // <-- flatten the struct field
    people.show()
  }
}
