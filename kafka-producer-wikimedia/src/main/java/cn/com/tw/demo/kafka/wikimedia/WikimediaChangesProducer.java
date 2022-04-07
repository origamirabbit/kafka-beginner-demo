package cn.com.tw.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String WIKIMEDIA_TOPIC = "wikimedia.recentchange";
    public static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        // create Producer Properties
        final var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set safe producer configs (kafka <= 2.8)
        // properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // max.block.ms & buffer.memory
        /*
         * If the producer produces faster than the broker can taskm the records will be buffered in memory
         *
         * buffer.memory=33554432 (32MB):the size of the send-buffer
         *
         * That buffer will fill up over time and empty back down when the throughput to the broke increases
         *
         * If that buffer is full (all 32MB), then the .send() method will block until the buffer empties
         *
         * max.block.ms=60000:the time the .send() will block until throwing an exception.
         * Exceptions are thrown when:
         * - The producer has filled up its buffer
         * - THe broker is not accepting any new data
         * - 60 seconds has elapsed
         *
         * If you hit an exception that usually means your brokers are down or overloaded as they can't respond to requests.
         */

        // create the Producer
        final var producer = new KafkaProducer<String, String>(properties);

        EventHandler eventHandler = new WikimediachangeHandler(producer, WIKIMEDIA_TOPIC);
        final var builder = new EventSource.Builder(eventHandler, URI.create(WIKIMEDIA_URL));
        final var eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // produce messages for 10 minutes and block the main thread
        TimeUnit.MINUTES.sleep(10);
    }
}
