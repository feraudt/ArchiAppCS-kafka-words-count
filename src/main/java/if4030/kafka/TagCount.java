/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.kafka.streams.examples.wordcount;
package if4030.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
public final class TagCount {

    public static final String INPUT_TOPIC = "tagged-words-stream";
    public static final String OUTPUT_TOPIC = "count-stream";
    public static final String COMMAND_TOPIC = "command-topic";

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "tagcount");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createCountStream(
        final StreamsBuilder builder,
        String[] excludedCategories,
        Map<String,PriorityQueue<Map<String,Long>>> catDict,
        Comparator<Map<String, Long>> comparator)
    {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KTable<String[], Long> countTable = source
            .filter((cat, lemme) -> !Arrays.asList(excludedCategories).contains(cat))
            .groupBy((cat, lemme) -> new String[] { cat, lemme })
            .count();
        
        KStream<String[], Long> countStream = countTable.toStream();
        countStream.peek(( key, value ) -> System.out.println( "Count stream - key: " + key + " - value: " + value ));
        countStream.foreach((catLemme, count) -> {
                if (!catDict.containsKey(catLemme[0])) {
                    catDict.put(catLemme[0], new PriorityQueue<>(20, comparator));
                }
                Map<String, Long> lemmeCount = new HashMap<>();
                lemmeCount.put(catLemme[1], count);
                PriorityQueue<Map<String,Long>> queue = catDict.get(catLemme[0]);
                queue.add(lemmeCount);
                catDict.replace(catLemme[0], queue);
            });
    }

    static void createCommandStream(
        final StreamsBuilder builder,
        Map<String,PriorityQueue<Map<String,Long>>> catDict,
        final KafkaStreams[] streamsList,
        final CountDownLatch latch)
    {
            final KStream<String, String> commandStream = builder.stream(COMMAND_TOPIC);
            commandStream.peek(( key, value ) -> System.out.println( "Command stream - key: " + key + " - value: " + value ));
            commandStream.filter((key, value) -> value.equals("END")).foreach((key, value) -> terminationHandler(catDict, streamsList, latch));
    }

    static void terminationHandler(
        Map<String,PriorityQueue<Map<String,Long>>> catDict,
        final KafkaStreams[] streamsList,
        final CountDownLatch latch)
    {
        System.out.println( "Terminating..." );
        Iterator<String> keyIterator = catDict.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            PriorityQueue<Map<String,Long>> queue = catDict.get(key);
            System.out.println("Top " + queue.size() + " " + key + " :");
            Iterator<Map<String,Long>> queueIterator = queue.iterator();
                while (queueIterator.hasNext()) {
                    System.out.println(queueIterator.next().keySet().iterator().next());
                }
        }
        streamsList[0].close();
        streamsList[1].close();
        latch.countDown();
        System.exit(0);
        System.out.println( "Terminated with success" );
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);

        String[] excludedCategories = new String[] { "PRO", "ART" }; // Ignore pronouns and articles
        Map<String,PriorityQueue<Map<String,Long>>> catDict = new HashMap<>();
        Comparator<Map<String, Long>> comparator = new Comparator<Map<String, Long>>() {
            @Override
            public int compare(Map<String, Long> map1, Map<String, Long> map2) {
                // Get the values of the maps
                Long value1 = map1.values().iterator().next();
                Long value2 = map2.values().iterator().next();
                // Compare the values
                if (value1 < value2) {
                    return -1;
                } else if (value1 > value2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };

        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams[] streamsList = new KafkaStreams[2];

        final StreamsBuilder builder = new StreamsBuilder();
        createCountStream(builder, excludedCategories, catDict, comparator);

        final StreamsBuilder commandBuilder = new StreamsBuilder();
        createCommandStream(commandBuilder, catDict, streamsList, latch);

        streamsList[0] = new KafkaStreams(builder.build(), props);
        streamsList[1] = new KafkaStreams(commandBuilder.build(), props);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-tagcount-shutdown-hook") {
            @Override
            public void run() {
                streamsList[0].close();
                streamsList[1].close();
                latch.countDown();
            }
        });

        try {
            streamsList[0].start();
            streamsList[1].start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
