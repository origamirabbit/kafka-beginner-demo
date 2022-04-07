package cn.com.tw.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediachangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    private final Logger log = LoggerFactory.getLogger(WikimediachangeHandler.class.getSimpleName());

    public WikimediachangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // no-op
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info(messageEvent.getData());

        // asynchronously send message to kafka
        // If the buffer is full, the message will be blocked until the buffer has room.
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

        log.info("Message sent to kafka topic: {}", topic);
    }

    @Override
    public void onComment(String comment) {
        // no-op
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in wikimedia event handler", t);
    }
}
