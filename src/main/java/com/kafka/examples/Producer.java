package com.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class Producer {

    public static void main(Properties props, String[] argv) throws IOException {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        try {
            for (int i = 0; i < 1000000; i++) {

                String message = String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i);

                // send lots of messages
                producer.send(new ProducerRecord<>(
                        "fast-messages",
                        message));

                // every so often send to a different topic
                if (i % 1000 == 0) {

                    message = String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i);

                    String marker = String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i);

                    producer.send(new ProducerRecord<String, String>(
                            "fast-messages",
                            message));
                    producer.send(new ProducerRecord<String, String>(
                            "summary-messages",
                            marker));
                    producer.flush();

                    System.out.println("Sent msg number " + i + " " + message);
                    System.out.println("Sent marker " + marker);
                }

            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }
    }

}
