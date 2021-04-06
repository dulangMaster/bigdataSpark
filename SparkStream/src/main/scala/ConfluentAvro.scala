import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ConfluentAvro {


  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("KafkaStreaming").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(10))

    //准备参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:6667",
      "schema.registry.url" -> "http://node2:8081",
      "key.deserializer" -> classOf[KafkaAvroDeserializer],
      "value.deserializer" -> classOf[KafkaAvroDeserializer],
      "group.id" -> "consumer-group8",
     //"auto.offset.reset" -> "latest", // 初次启动从最开始的位置开始消费
    "auto.offset.reset" -> "earliest", // 初次启动从最开始的位置开始消费
      //Kafka提供的有api，可以将offset提交到指定的kafkatopic或者选择checkpoint的方式，下面会说
    //自己来决定什么时候提交offset
     "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //上面通过Kafka提供的有api设置过offset
    // ssc.checkpoint("/flume_streaming/data")

    //topics（注意这里是复数哦）
    val topics = Array("mysql-test-test_confulent")
    //通过KafkaUtils放回一个DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    // 处理每个微批的rdd
    kafkaStream.foreachRDD( rdd => {
      if(rdd!=null && !rdd.isEmpty()){
        //获取偏移量
        val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //对每个分区分别处理
        rdd.foreachPartition( iterator  => {
          if( iterator != null && !iterator.isEmpty ){
            //作相应的处理
            while (iterator.hasNext) {
              //处理每一条记录
              val next = iterator.next
              //这个就是接收到的数据值对象，
              val record = next.value().asInstanceOf[Record]

              println(">>>>>>>>>>>" +record)
              //可以插入数据库，或者输出到别的地方

            }
          }
        })
        //将偏移量提交（偏移量是提交到kafka管理的）
       kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
        println("submit offset!")
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }


//
//  def parseAVROToString(rawTweet: Array[Byte]): String = {
//    if (rawTweet.isEmpty) {
//     println("null")
//    } else {
//      deserializeTwitter(rawTweet).get("tweet").toString
//    }
//    ""
//  }
//  def deserializeTwitter(tweet: Array[Byte]): GenericRecord = {
//    try {
//      val reader = new GenericDatumReader[GenericRecord](tweetSchema)
//      val decoder = DecoderFactory.get.binaryDecoder(tweet, null)
//      reader.read(null, decoder)
//    } catch {
//      case e: Exception => None
//        null;
//    }
//  }

}
