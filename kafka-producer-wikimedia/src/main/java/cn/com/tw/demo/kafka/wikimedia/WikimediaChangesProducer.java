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
