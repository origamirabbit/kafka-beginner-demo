package cn.com.tw.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String GROUP_ID = "my-consumer-group-0";
    public static final String EARLIEST = "earliest";
    public static final String HELLO_TOPIC = "hello-topic";

    public static void main(String[] args) {
        log.info("==========ConsumerDemo==========");

        // create Consumer Properties
        final var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);

        // create the Consumer
        final var consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        // consumer.subscribe(Collections.singleton(HELLO_TOPIC));
        consumer.subscribe(List.of(HELLO_TOPIC));

        // poll for new data
        while (true) {
            log.info("=====Polling for new data...=====");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1001));

            for (ConsumerRecord<String, String> record : records) {
                log.info(MessageFormat.format(
                        "Received message: ({0}, {1}) in Patition {2} at offset {3}",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset())
                );
            }
        }

        // flush and close the Consumer
        // consumer.close();
    }
}
