import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}

object AvroCorrect {

  import org.apache.spark.sql.avro.{SchemaConverters, from_avro, to_avro}
  import org.apache.spark.sql.DataFrame
  val spark: SparkSession = SparkSession.builder().master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val input1 = spark.createDataset(Seq(
      """
          {
                "id": "Jacek",
                "name": "Laskowski",
                "count": "1111@japila.pl",
                "addr": "wwwww"
              }
      """
    )).toDF("value")
    val schema = new StructType()
        .add($"id".string)
        .add($"name".string)
        .add($"count".string)
        .add($"addr".string)

    val people = input1
      .select(from_json($"value", schema) as "key")
//      .select("json.addresses.city") // <-- flatten the struct field
    val input2 = spark.createDataFrame(people.rdd, people.schema)


    println("############### testing .toDF()")
    test_avro(people)
    println("############### testing .createDataFrame()")
    test_avro(input2)
  }
  def test_avro(df: DataFrame): Unit = {
    println("input df:")
    df.printSchema()
    df.show()

    val keySchema = SchemaConverters.toAvroType(df.schema).toString
    println(s"avro schema: $keySchema")
    import spark.implicits._
    val avroDf = df
      .select(to_avro($"key") as "key")

    println("avro serialized:")
    avroDf.printSchema()
    avroDf.show()

    val output = avroDf
      .select(from_avro($"key", keySchema) as "key")
//      .select("key.*")

    println("avro deserialized:")
    output.printSchema()
    output.show()
    val out2 = output.select("key.key.name")
    out2.printSchema()
    out2.show()
  }

}
