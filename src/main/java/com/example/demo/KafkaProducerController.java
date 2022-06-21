package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RestController
public class KafkaProducerController {

    private static final String KEY = "FIXED-KEY";

    private static final Map<Integer, String> LEFT;
    static {
        LEFT = new HashMap<>();
        LEFT.put(1, null);
        LEFT.put(3, "3,A");
        LEFT.put(5, "5,B");
        LEFT.put(7, null);
        LEFT.put(9, "9,C");
        LEFT.put(12, null);
        LEFT.put(15, "15,x");
    }

    private static final Map<Integer, String> RIGHT;
    static {
        RIGHT = new HashMap<>();
        RIGHT.put(2, null);
        RIGHT.put(3, "a");
        RIGHT.put(5, "b");
        RIGHT.put(8, null);
//        RIGHT.put(9, "c");
        RIGHT.put(11, null);
        RIGHT.put(13, null);
        RIGHT.put(15, "D");
    }

    @RequestMapping("/sendMessages/")
    public void sendMessages() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092,localhost:39092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 15; i++) {
                // Every 10 seconds send a message
//                try {
//                    Thread.sleep(500);
//                } catch (InterruptedException e) {}

                if (LEFT.containsKey(i + 1)) {
                    producer.send(new ProducerRecord<Integer, String>("my-kafka-left-stream-topic", (i + 1), LEFT.get(i + 1)));
                }
                if (RIGHT.containsKey(i + 1)) {
                    producer.send(new ProducerRecord<Integer, String>("my-kafka-right-stream-topic", (i + 1), RIGHT.get(i + 1)));
                }

            }
        } finally {
            producer.close();
        }

    }

}