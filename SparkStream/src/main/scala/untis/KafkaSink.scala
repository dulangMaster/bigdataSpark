package untis

import java.util.Properties

import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{ForeachWriter, Row}

class KafkaSink (topic:String, server:String) extends  org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
  val kafkaProperties= new Properties()
  kafkaProperties.put("bootstrap.servers",server)
  kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  var producer: KafkaProducer[String,String] = _
   def open(partitionId: Long, epochId: Long):Boolean = {
    producer = new KafkaProducer(kafkaProperties)
    true
  }

   def process(value: Row): Unit = {
    producer.send( new ProducerRecord(topic,value(0)+":"+value(1)))
  }

  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }

}
