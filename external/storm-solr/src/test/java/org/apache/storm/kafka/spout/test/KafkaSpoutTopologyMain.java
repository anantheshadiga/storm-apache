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
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KafkaSpoutTopologyMain {
    public static void main(String[] args) throws InterruptedException {
//        stopOnInput();
        submitTopologyLocalCluster(getTopolgy(), getConfig());
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

}
