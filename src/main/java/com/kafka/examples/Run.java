package com.kafka.examples;

import com.google.common.io.Resources;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Run {



    public static void main(String[] argv) throws IOException {

        if (argv.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
        }

        Properties properties = new Properties();

        switch (argv[0]) {
            case "producer":
                try (InputStream props = Resources.getResource("producer.properties").openStream()) {
                    properties.load(props);
                }

                Producer.main(properties, argv);
                break;
            case "consumer":

                try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
                    properties.load(props);
                }

                Consumer.main(properties, argv);
                break;
            case "stream":

                try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
                    properties.load(props);
                }

                Stream.main(properties);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + argv[0]);
        }
    }
}
