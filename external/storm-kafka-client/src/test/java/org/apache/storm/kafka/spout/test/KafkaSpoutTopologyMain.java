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

package org.apache.storm.kafka.spout.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ExponentialBackoffRetry;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.RetryService;
import org.apache.storm.kafka.spout.TopicTest2TupleBuilder;
import org.apache.storm.kafka.spout.TopicsTest0Test1TupleBuilder;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class KafkaSpoutTopologyMain {
    private static final String[] STREAMS = new String[]{"test0_stream","test1_stream","test2_stream"};
    private static final String[] TOPICS = new String[]{"test0","test1","test2"};


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            submitTopologyLocalCluster(getTopolgyKafkaSpout(), getConfig());
        } else {
            submitTopologyRemoteCluster(args[0], getTopolgyKafkaSpout(), getConfig());
        }
    }

    protected static void submitTopologyLocalCluster(StormTopology topology, Config config) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        stopWaitingForInput();
    }

    protected static void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    private static void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    public static StormTopology getTopolgyKafkaSpout() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams())), 1);
        tp.setBolt("kafka_bolt", new KafkaTestBolt()).shuffleGrouping("kafka_spout", STREAMS[0]);
        tp.setBolt("kafka_bolt_1", new KafkaTestBolt()).shuffleGrouping("kafka_spout", STREAMS[2]);
        return tp.createTopology();
    }

    public static KafkaSpoutConfig<String,String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams) {
        return new KafkaSpoutConfig.Builder<String, String>(getKafkaConsumerProps(), kafkaSpoutStreams, getTuplesBuilder(), getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    private static RetryService getRetryService() {
        return new ExponentialBackoffRetry(getDelay(5, TimeUnit.MILLISECONDS), 2, Integer.MAX_VALUE, getDelay(10, TimeUnit.SECONDS));
    }

    private static ExponentialBackoffRetry.Delay getDelay(long delay, TimeUnit timeUnit) {
        return new ExponentialBackoffRetry.Delay(delay, timeUnit);
    }

    public static Map<String,Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
//        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "true");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static KafkaSpoutTuplesBuilder getTuplesBuilder() {
        return new KafkaSpoutTuplesBuilder.Builder(
                new TopicsTest0Test1TupleBuilder<>(TOPICS[0], TOPICS[1]),
                new TopicTest2TupleBuilder<>(TOPICS[2]))
                .build();
    }

    public static KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
        final Fields outputFields1 = new Fields("topic", "partition", "offset");
        return new KafkaSpoutStreams.Builder(outputFields, STREAMS[0], new String[]{TOPICS[0], TOPICS[1]})  // contents of topics test0, test1, sent to test0_stream
                .addStream(outputFields, STREAMS[0], new String[]{TOPICS[2]})  // contents of topic test2 sent to test0_stream
                .addStream(outputFields1, STREAMS[2], new String[]{TOPICS[2]})  // contents of topic test2 sent to test2_stream
                .build();
    }
}
