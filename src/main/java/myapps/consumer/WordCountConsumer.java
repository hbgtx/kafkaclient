package myapps.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static myapps.utils.ConstantUtils.BOOTSTRAP_SERVER;
import static myapps.utils.ConstantUtils.WORDCOUNT_OUTPUT_TOPIC;

public class WordCountConsumer {

    private static final String GROUP_ID = "WC_consumer";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(WORDCOUNT_OUTPUT_TOPIC));


        while (true) {
            ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Long> record : records) {
                System.out.println("Word:" + record.key() + ", came " + record.value() + " times");
                if (record.key().equals("closeapp")) {
                    System.exit(0);
                }
            }
        }

    }
}
