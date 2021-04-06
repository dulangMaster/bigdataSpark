package avro;

import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @Title ConfluentConsumer.java
 * @Description 使用Confluent实现的Schema Registry服务来消费Avro序列化后的对象
 * @Author YangYunhe
 * @Date 2018-06-25 11:42:21
 */
public class ConfluentConsumer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:6667,node2:6667");
        props.put("group.id", "dev3-yangyunhe-group002");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 使用Confluent实现的KafkaAvroDeserializer
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        // 添加schema服务的地址，用于获取schema
        props.put("schema.registry.url", "http://node2:8081");
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("mysql-test-test_confulent2"));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord user = record.value();
                    System.out.println("value = [user.id = " + user.get("id") + ", " + "user.name = "
                            + user.get("name") + ", " + "user.count = " + user.get("count") + "], "
                            + "partition = " + record.partition() + ", " + "offset = " + record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
