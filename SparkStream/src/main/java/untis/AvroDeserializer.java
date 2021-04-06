package untis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;


/**
 * @Title AvroDeserializer.java
 * @Description 使用传统的 Avro API 自定义反序列类
 * @Author YangYunhe
 * @Date 2018-06-21 17:19:40
 */
//public class AvroDeserializer implements Deserializer<Stock> {
//
//    @Override
//    public void close() {}
//
//    @Override
//    public void configure(Map<String, ?> arg0, boolean arg1) {}
//
//    @Override
//    public Stock deserialize(String topic, byte[] data) {
//        if(data == null) {
//            return null;
//        }
//        Stock stock = new Stock();
//        ByteArrayInputStream in = new ByteArrayInputStream(data);
//        DatumReader<Stock> userDatumReader = new SpecificDatumReader<>(stock.getSchema());
//        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
//        try {
//            stock = userDatumReader.read(null, decoder);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return stock;
//    }
// }

