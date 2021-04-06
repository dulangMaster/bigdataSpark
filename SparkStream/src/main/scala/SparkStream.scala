import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkStream {
  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val sparkConf=new SparkConf().setAppName("KafkaStreaming").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(10))

    //准备参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group",
      //Kafka提供的有api，可以将offset提交到指定的kafkatopic或者选择checkpoint的方式，下面会说
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //上面通过Kafka提供的有api设置过offset
    // ssc.checkpoint("/flume_streaming/data")

    //topics（注意这里是复数哦）
    val topics = Array("test")
    //通过KafkaUtils放回一个DStream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //stream.map(record => (record.key, record.value)).print()

    //wc
    val lines=stream.map(_.value)
    val word=lines.flatMap(_.split(",")).map(x=>(x,1)).reduceByKey(_+_).print


    ssc.start()
    ssc.awaitTermination()
  }
}
