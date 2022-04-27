package pl.gatomek;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class GatomekKafkaProducer {

    public static void main(String[] args) {
        String bootstrapServers = "dev.local:9092";

        Properties props = new Properties();

        props.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServers);
        props.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer( props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>( "test", Long.toString(System.currentTimeMillis()));
        producer.send( record);

        producer.close();
    }
}
