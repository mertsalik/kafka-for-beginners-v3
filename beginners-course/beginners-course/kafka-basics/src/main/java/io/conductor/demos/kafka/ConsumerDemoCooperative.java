package io.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        // System.out.println("Hello World!");
        log.info("I'm a Consumer!");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        // create Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe Consumer to our topics
        consumer.subscribe(Collections.singleton(topic));

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                super.run();
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // poll for new data
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> consumerRecord : records) {
                    log.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                    log.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
                }
            }
        } catch (WakeupException wakeupException) {
            log.info("Wake up Exception!");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            consumer.close(); // this will also commit the offsets if need be
            log.info("The consumer is now gracefully closed.");
        }
    }
}
