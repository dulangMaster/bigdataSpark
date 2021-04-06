package untis

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringDeserializer

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
          // 返回空字符串
          null
        } else {
          genericRecord.toString

        }
    }
  }
}
