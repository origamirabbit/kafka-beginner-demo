package cn.com.tw.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws InterruptedException {
        log.info("==========ProducerDemoWithKeys==========");

        // create Producer Properties
        final var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        final var producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // czreate a ProducerRecord
            final var topic = "hello-topic";
            final var key = "id_" + i;
            final var value = "hello world" + i;
            final var producerRecord = new ProducerRecord<>(topic, key, value);

            // send data - asynchronous | second parameter is a callback()
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info(MessageFormat.format(
                            "Received new metadata.\nTopic: {0}\nKey: {1}\nPartition: {2}\nOffset: {3}\nTimestamp: {4}",
                            metadata.topic(),
                            producerRecord.key(),
                            metadata.partition(),
                            metadata.offset(),
                            metadata.timestamp()));
                } else {
                    log.error("=====Error while producing=====!", exception);
                }
            });

            // Round-robin to all partitions - comment out to send to a single partition (Sticky Partitioning)
            // Thread.sleep(1000);
        }
        // flush data - synchronous
        producer.flush();

        // flush and close the Producer
        producer.close();
    }
}
