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
import org.apache.storm.kafka.spout.strategy.KafkaConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaSpout<K,V> extends BaseRichSpout {
    protected SpoutOutputCollector collector;
    private final KafkaConfig<K, V> kafkaConfig;
    private KafkaConsumer<K, V> kafkaConsumer;

    public KafkaSpout(KafkaConfig<K,V> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;                 // Pass in configuration
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConfigs(), kafkaConfig.getKeyDeserializer(), kafkaConfig.getValueDeserializer());

        List<String> topics = new ArrayList<>();    // TODO
        subscribe(kafkaConsumer);

        kafkaConsumer.subscribe(topics);
//        kafkaConsumer.seek();6
//        kafkaConsumer.commitSync();
    }


    ConcurrentMap<String, ConsumerRecord> topicToPendingRecords = new ConcurrentHashMap<>();

    @Override
    public void nextTuple() {
        if (getFailedTuples() + getPendingTuples() < getMaxPendingTuples()) {
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaConfig.getPollTimeout());
            consumerRecords.count();
            consumerRecords.partitions();
            Iterable<ConsumerRecord<K, V>> records = consumerRecords.records("bla");
            for (ConsumerRecord<K, V> record : records) {
                record.offset();

            }


            kafkaConsumer.commitSync();
            // poll tuples
        }

        // poll

        // emit when pendingTuples > maxPendingTuples || wait time > maxWaitTime
            // new tuples
            // failed tuples

        kafkaConsumer.subscribe();
        ConsumerRecords<K,V> consumerRecords = kafkaConsumer.poll(kafkaConfig.getPollTimeout());

        kafkaConsumer.

        process(consumerRecords, collector);


        consumerRecords.partitions();

        for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
            consumerRecord.key();
            consumerRecord.partition();
            consumerRecord.offset();
            consumerRecord.topic();
            consumerRecord.value();
        }

        kafkaConsumer.commitSync();

        collector.emit(getStreamId(), getTuple(), getMessageId(con));
    }

    private void getOutputFields1() {

    }

    private Values getTuple() {
        return null;
    }

    private Object getMessageId(ConsumerRecord<K,V> consumerRecord) {
        return null;
    }

    private void serialize() {

    }

    private String getStreamId() {
        return null;
    }


    private static class MessageId {
        private String topic;
        private int partition;
        private long offset;

        public MessageId(ConsumerRecord consumerRecord) {
            this(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        }

        public MessageId(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final MessageId messageId = (MessageId) o;
            if (partition != messageId.partition) {
                return false;
            }

            if (offset != messageId.offset) {
                return false;
            }

            return topic.equals(messageId.topic);
        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + partition;
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (kafkaSpoutStrategy.getDeclaredStreamsAndOutputFields() != null)     //TODO
            declarer.declare(getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields("kafka_field");
    }

    @Override
    public void ack(Object msgId) {
        // commit message
        LoggerHugo.doLog("Spout acked");
    }

    @Override
    public void fail(Object msgId) {

        LoggerHugo.doLog("Spout failed");
    }

    @Override
    public void activate() {
        //resume processing
    }

    @Override
    public void deactivate() {
        //commit
    }

    @Override
    public void close() {
        //remove resources
    }
}
