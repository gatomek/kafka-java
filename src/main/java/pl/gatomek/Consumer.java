package pl.gatomek;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger( Consumer.class);

        String bootstrapServers = "192.168.18.10:9093";
        String groupId = "c0";
        String topic = "test";

        Properties props = new Properties();

        props.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServers);
        props.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty( ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>( props);

        consumer.subscribe(Arrays.asList( topic));

        while( true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis( 100));
            for(ConsumerRecord<String, String> r : records)
                logger.info( r.topic() + " | " + r.value() + " | " + r.offset() + " | " + r.partition());
        }
    }
}
