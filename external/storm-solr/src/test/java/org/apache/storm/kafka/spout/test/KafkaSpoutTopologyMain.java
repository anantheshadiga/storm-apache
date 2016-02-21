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

import com.google.common.collect.Lists;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.strategy.KafkaRecordTupleBuilder;
import org.apache.storm.kafka.spout.strategy.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.strategy.KafkaSpoutStream;
import org.apache.storm.kafka.spout.strategy.KafkaTupleBuilder;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaSpoutTopologyMain {

    public static void main(String[] args) throws Exception {
//        stopOnInput();
//        submitTopologyLocalCluster(getTopolgy(), getConfig());

        if (args.length == 0) {
            submitTopologyLocalCluster(getTopolgyKafkaSpout(), getConfig());
        } else {
            submitTopologyRemoteCluster(args[1], getTopolgyKafkaSpout(), getConfig());
        }
    }

    protected static void submitTopologyLocalCluster(StormTopology topology, Config config) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
//        Thread.sleep(1000000);
//        System.out.println("Killing topology per client's request");
//        cluster.killTopology("test");
//        cluster.shutdown();
//        System.exit(0);
        stopOnInput();
    }

    protected static void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    private static void stopOnInput() {
        try {
            System.out.println("PRESSE ENTER TO STOP");
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


    public static StormTopology getTopolgy() {
        TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("hmcl_kafka_spout", new KafkaTestSpout(), 10);
        tp.setBolt("hmcl_kafka_bolt", new KafkaTestBolt()).shuffleGrouping("hmcl_kafka_spout");
        return tp.createTopology();
    }

    public static StormTopology getTopolgyKafkaSpout() {
        TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("hmcl_kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(), getKafkaOutputStream(), getTupleBuilder()), 1);
        tp.setBolt("hmcl_kafka_bolt", new KafkaTestBolt()).shuffleGrouping("hmcl_kafka_spout");
        return tp.createTopology();
    }

    public static KafkaSpoutConfig<String,String> getKafkaSpoutConfig() {
        return new KafkaSpoutConfig.Builder<String, String>(getKafkaConsumerProps(), getTopics()).setCommitFreqMs(5_000).build();
    }

    public static Map<String,Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "false");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static List<String> getTopics() {
        return Lists.newArrayList("test");
    }

    public static KafkaTupleBuilder<String,String> getTupleBuilder() {
        return new KafkaRecordTupleBuilder<>();
    }

    public static KafkaSpoutStream getKafkaOutputStream() {
        return new KafkaSpoutStream(new Fields("topic", "partition", "offset", "key", "value"));
    }

}
