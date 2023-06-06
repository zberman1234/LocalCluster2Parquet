import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {
    private static final String TOPIC_NAME = "my_topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String OUTPUT_PATH = "parquet/";  // Specify the directory to store Parquet files


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_consumer_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value());
                // Here, you can write the received message to Parquet files using a Parquet writer library.
                writeMessageToParquet(record.value());
            }
        }
    }

    private static void writeMessageToParquet(String message) {
        try {
            Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Message\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}");

            GenericRecord record = new GenericData.Record(schema);
            record.put("message", message);

            DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
                    .<GenericRecord>builder(new Path(OUTPUT_PATH + System.currentTimeMillis() + ".parquet"))
                    .withSchema(schema)
                    .withConf(new org.apache.hadoop.conf.Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build();

            parquetWriter.write(record);
            parquetWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
