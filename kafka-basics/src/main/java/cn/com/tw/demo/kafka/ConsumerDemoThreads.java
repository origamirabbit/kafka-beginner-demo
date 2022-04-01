package cn.com.tw.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String GROUP_ID = "my-consumer-group-hello";
    public static final String EARLIEST = "earliest";
    public static final String HELLO_TOPIC = "hello-topic";

    // Running a Java Consumer in a separate thread allows you to perform other tasks in the main thread.
    public static void main(String[] args) {
        final var consumerDemoWorker = new ConsumerDemoWorker();
        new Thread(consumerDemoWorker).start();
        Runtime.getRuntime().addShutdownHook(new Thread(new ConsumerDemoCloser(consumerDemoWorker)));
    }

    private static class ConsumerDemoWorker implements Runnable {
        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWorker.class);
        private CountDownLatch countDownLatch;
        private Consumer<String, String> consumer;

        @Override
        public void run() {
            log.info("==========ConsumerDemoThreads==========");

            // create CountDownLatch
            countDownLatch = new CountDownLatch(1);
            // create Consumer Properties
            final var properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);

            // create the Consumer
            this.consumer = new KafkaConsumer<>(properties);

            // subscribe consumer to our topic(s)
            this.consumer.subscribe(Collections.singleton(HELLO_TOPIC));

            final var pollTimeout = Duration.ofMillis(100);

            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(pollTimeout);

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
                this.consumer.close();
                this.countDownLatch.countDown();
                log.info("Closed the consumer gracefully");
            }
        }

        void shutdown() throws InterruptedException {
            this.consumer.wakeup();
            this.countDownLatch.await();
            log.info("Closed the consumer");
        }
    }

    private static class ConsumerDemoCloser implements Runnable {
        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCloser.class);

        private final ConsumerDemoWorker consumerDemoWorker;

        ConsumerDemoCloser(ConsumerDemoWorker consumerDemoWorker) {
            this.consumerDemoWorker = consumerDemoWorker;
        }

        @Override
        public void run() {
            try {
                this.consumerDemoWorker.shutdown();
            } catch (InterruptedException e) {
                log.error("Unexpected Exception while shutting down: {}", e.getMessage());
            }
        }
    }
}
