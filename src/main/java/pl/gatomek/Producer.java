package pl.gatomek;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class Producer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Producer.class);

        String bootstrapServers = "192.168.18.10:9093";
        String ackConfig = "0";
        String topic = "test";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, ackConfig);

        // safe producer
        // ...

        // high throuhtput producer
        // props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
        // props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, UUID.randomUUID().toString());
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null)
                    logger.info("METADATA: " + "Topic: " + recordMetadata.topic() + " | Partition: " + recordMetadata.partition() + " | Offset: " + recordMetadata.offset());
            }
        });

        producer.close();
    }
}
