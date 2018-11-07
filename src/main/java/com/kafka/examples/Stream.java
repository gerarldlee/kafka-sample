package com.kafka.examples;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

public class Stream {
    public static void main(Properties props, String[] argv) throws IOException {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("odd", "even");
        KStream<String, String> joined = stream
                .map((key, value) -> KeyValue.pair(key, key.concat(value)));
        joined.to("odd-even");
        KafkaStreams kafkaStreams = new KafkaStreams(builder, props);
        kafkaStreams.start();
    }
}
