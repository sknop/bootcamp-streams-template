package io.confluent.bootcamp.streams;

import io.confluent.bootcamp.common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

import java.io.*;
import java.util.Properties;

public class HelloWorld {

    static public void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Need property file");
            System.exit(1);
        }
        String propertyFile = args[0];
        Properties properties = new Properties();

        try (InputStream inputStream = new FileInputStream(propertyFile)) {
            Reader reader = new InputStreamReader(inputStream);

            properties.load(reader);
        } catch (FileNotFoundException e) {
            System.err.println("Cannot find file " + propertyFile);
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StreamsBuilder builder = new StreamsBuilder();

        // **************************
        // Your stream code goes here
        // **************************

        var stream = new KafkaStreams(builder.build(), properties);
        stream.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}
