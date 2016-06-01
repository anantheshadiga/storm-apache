/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Unit test for simple KafkaSpoutTest.
 */
public class KafkaConsumerBasicTest {
    @Test
    public void test_spoutFunctionality_expectedBehavior() throws Exception {
    }


    @Test
    public void main() {
        Properties props = getProperties();
//        System.out.println("Before consumer ");
//        System.err.println("Before consumer Err");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));

//        System.out.println("Before poll");
//        System.err.println("Before poll Err");

//        System.err.println("After poll");
        consumer.poll(1);
//        consumer.seekToBeginning(getTopicPartition());
        consumer.seek(getTopicPartition(), consumer.committed(getTopicPartition()).offset());
//        consumer.seekToEnd(getTopicPartition());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
//            consumer.seek(new TopicPartition("test", 0), 0);
            long offset = 0;
            for (ConsumerRecord<String, String> record : records) {
//                System.err.println("Inside Loop");
                System.err.printf("offset = %d, key = %s, value = %s\n", offset = record.offset(), record.key(), record.value());
            }

            System.out.println("sleeping");
            sleep(10_000);
//            consumer.seekToEnd(getTopicPartition());
//            consumer.seek(getTopicPartition(), consumer.position(getTopicPartition()));
//            consumer.seek(getTopicPartition(), 40L);

            System.out.println("awake");

//            consumer.commitSync(getOffsets(offset));
        }
//        System.err.println("Exit");
    }

    private void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsets(final long offset) {
        return new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(getTopicPartition(), new OffsetAndMetadata(offset, ""));
        }};
    }

    private TopicPartition getTopicPartition() {
        return new TopicPartition("test", 0);
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
//        props.put("bootstrap.servers", "localhost:9923");
        props.put("group.id", "test-group");
//        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
