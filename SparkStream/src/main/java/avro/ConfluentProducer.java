package avro;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static java.nio.file.Files.readAllBytes;

/**
 * @Title ConfluentProducer.java
 * @Description 使用Confluent实现的Schema Registry服务来发送Avro序列化后的对象
 * @Author YangYunhe
 * @Date 2018-06-25 10:49:19
 */
public class ConfluentProducer {

//    public static final String USER_SCHEMA = "{\"type\": \"record\", \"name\": \"User\", " +
//            "\"fields\": [{\"name\": \"id\", \"type\": \"int\"}, " +
//            "{\"name\": \"name\",  \"type\": \"string\"}, {\"name\": \"count\", \"type\": \"int\"}]}";
    public static  String jsonFormatSchema;

    static {
        try {
            jsonFormatSchema = new String(readAllBytes(Paths.get("avro/test.avsc")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:6667,node2:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 使用Confluent实现的KafkaAvroSerializer
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        // 添加schema服务的地址，用于获取schema
        props.put("schema.registry.url", "http://node2:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(jsonFormatSchema);

        Random rand = new Random();
        int id = 0;

        while(id < 100) {
            id++;
            String name = "names" + id;
            int age = rand.nextInt(40) + 1;
            GenericRecord user = new GenericData.Record(schema);
            user.put("id", id);
            user.put("name", name);
            user.put("count", age);
            user.put("modified", new Date().getTime());

            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("mysql-test-test_confulent", user);
            producer.send(record);
            Thread.sleep(1000);
        }
        producer.close();
    }

}