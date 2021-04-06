package untis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;



/**
 * @Title AvroSerializer.java
 * @Description 使用传统的 Avro API 自定义序列化类
 * @Author YangYunhe
 * @Date 2018-06-21 16:40:35
 */
//public class AvroSerializer implements Serializer<Stock> {
//
//    @Override
//    public void close() {}
//
//    @Override
//    public void configure(Map<String, ?> arg0, boolean arg1) {}
//
//    @Override
//    public byte[] serialize(String topic, Stock data) {
//        if(data == null) {
//            return null;
//        }
//        DatumWriter<Stock> writer = new SpecificDatumWriter<>(data.getSchema());
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
//        try {
//            writer.write(data, encoder);
//        }catch (IOException e) {
//            throw new SerializationException(e.getMessage());
//        }
//        return out.toByteArray();
//    }

//}
