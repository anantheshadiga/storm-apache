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
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.strategy.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.strategy.KafkaStream;
import org.apache.storm.kafka.spout.strategy.KafkaTupleBuilder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaSpout<K,V> extends BaseRichSpout {
    private static final Logger log = LoggerFactory.getLogger(KafkaSpout.class);

    // Storm
    private Map conf;
    private TopologyContext context;
    protected SpoutOutputCollector collector;

    // Kafka
    private final KafkaSpoutConfig<K, V> kafkaSpoutConfig;
    private KafkaConsumer<K, V> kafkaConsumer;

    // Bookkeeping
    private OffsetsManager offsetsManager;
    Map<MessageId, Values> emittedTuples;           // Keeps a list of emitted tuples that are pending being acked
    private KafkaStream<K,V> kafkaStream;
    private ScheduledExecutorService offsetsCommitTimer;
    private KafkaTupleBuilder<K,V> kafkaTupleBuilder;

    public KafkaSpout(KafkaSpoutConfig<K,V> kafkaSpoutConfig) {
        this.kafkaSpoutConfig = kafkaSpoutConfig;                 // Pass in configuration
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        kafkaConsumer = new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(), kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
        offsetsManager = new OffsetsManager(this, kafkaConsumer);
        emittedTuples = new HashMap<>();

      /*  List<String> topics = new ArrayList<>();    // TODO
        subscribe(kafkaConsumer);
        kafkaConsumer.subscribe(topics);*/

        setOffsetsCommitTask();
    }

    /***/
    private void setOffsetsCommitTask() {
        offsetsCommitTimer = Executors.newSingleThreadScheduledExecutor();
        offsetsCommitTimer.schedule(new Runnable() {
            @Override
            public void run() {
                commitAckedRecords();
            }
        }, 100, TimeUnit.MILLISECONDS);
    }

    //TODO HANDLE PARALLELISM
//    String topologyMaxSpoutPending = Config.TOPOLOGY_MAX_SPOUT_PENDING;
    @Override
    public void nextTuple() {
        if (retry()) {
            retryFailedTuples();
        } else {
            emitTuples(poll());
            pollNewRecordsAndEmitTuples();
        }
    }

    private void emitTuples(ConsumerRecords<K, V> consumerRecords) {
        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if emmit per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                final Values tuple = kafkaTupleBuilder.buildTuple(tp, record);
                final MessageId messageId = createMessageId(record);            //TODO don't create message for
                collector.emit(kafkaStream.getStreamId(), tuple, messageId);          // emits one tuple per record
                emittedTuples.put(messageId, tuple);
            }
        }
    }

    private boolean retry() {
        return offsetsManager.retry();
    }

    private void retryFailedTuples() {
        offsetsManager.retryFailed();
    }

    private void commitAckedRecords() {
        offsetsManager.commitAckedOffsets();
    }

    private ConsumerRecords<K, V> poll() {
        final ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeout());
        log.debug("Polled {[]} records from Kafka", consumerRecords.count());
        return consumerRecords;
    }

    //TODO: Null message id for no acking, which is good to use with enable.auto.commit=false
    private void pollNewRecordsAndEmitTuples() {
        ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeout());

        log.debug("Polled {[]} records from Kafka", consumerRecords.count());

        for (TopicPartition tp : consumerRecords.partitions()) {
            final Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(tp.topic());     // TODO Decide if emmit per topic or per partition
            for (ConsumerRecord<K, V> record : records) {
                final Values tuple = kafkaTupleBuilder.buildTuple(tp, record);
                final MessageId messageId = createMessageId(record);            //TODO don't create message for
                collector.emit(kafkaStream.getStreamId(), tuple, messageId);          // emits one tuple per record
                emittedTuples.put(messageId, tuple);
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        offsetsManager.ack((MessageId) msgId);
    }

    //TODO: HANDLE CONSUMER REBALANCE

    @Override
    public void fail(Object msgId) {
        offsetsManager.fail((MessageId) msgId);
    }

    private MessageId createMessageId(ConsumerRecord<K,V> consumerRecord) {
        return new MessageId(consumerRecord);
    }
    private String getStreamId() {
        return kafkaStream.getStreamId();
    }

    private final Lock ackedLock = new ReentrantLock();



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(kafkaStream.getStreamId(), kafkaStream.getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields();
    }   TODO

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
