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
import org.apache.storm.kafka.spout.strategy.KafkaSpoutStrategy;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaSpout extends BaseRichSpout {
    protected SpoutOutputCollector collector;
    private KafkaSpoutStrategy kafkaSpoutStrategy;
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaSpout(KafkaSpoutStrategy kafkaSpoutStrategy) {   // Pass in configuration
        this.kafkaSpoutStrategy = kafkaSpoutStrategy;

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
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        List<String> topics = new ArrayList<>();    // TODO
        kafkaConsumer.subscribe(topics);
    }

    @Override
    public void nextTuple() {
        long timeout = 10;          // TODO
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeout);
        consumerRecords.partitions();

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            consumerRecord.key();
            consumerRecord.partition();
            consumerRecord.offset();
            consumerRecord.topic();
            consumerRecord.value();
        }


        collector.emit(streamId, tuple, messageId);



    }

    @Override
    public void close() {
        //remove resources
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
    public void ack(Object msgId) {
        // commit message
        LoggerHugo.doLog("Spout acked");
    }

    @Override
    public void fail(Object msgId) {

        LoggerHugo.doLog("Spout failed");
    }
}
