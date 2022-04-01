package cn.com.tw.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String GROUP_ID = "my-consumer-group-hello";
    public static final String EARLIEST = "earliest";
    public static final String HELLO_TOPIC = "hello-topic";

    public static void main(String[] args) {
        log.info("==========ConsumerDemoWithShutdown==========");

        // create Consumer Properties
        final var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);

        // create the Consumer
        final var consumer = new KafkaConsumer<String, String>(properties);

        // get a reference to the current thread
        final var mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow th execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe consumer to our topic(s)
            // consumer.subscribe(Collections.singleton(HELLO_TOPIC));
            consumer.subscribe(List.of(HELLO_TOPIC));

            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(MessageFormat.format(
                            "Received message: ({0}, {1}) in Partition {2} at offset {3}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset())
                    );
                }
            }
        } catch (WakeupException e) {
            // ignore this as this is an expected exception when closing a consumer
            log.info("WakeupException: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected Exception: {}", e.getMessage());
        } finally {
            // Gracefully close the consumer
            consumer.close();
            log.info("Closed the consumer gracefully");
        }
    }
}
