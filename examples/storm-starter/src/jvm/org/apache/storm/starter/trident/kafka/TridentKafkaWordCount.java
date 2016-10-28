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
package org.apache.storm.starter.trident.kafka;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.starter.trident.DebugMemoryMapState;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * A sample word count trident topology using transactional kafka spout that has the following components.
 * <ol>
 * <li> {@link KafkaBolt}
 * that receives random sentences from {@link RandomSentenceSpout} and
 * publishes the sentences to a kafka "test" topic.
 * </li>
 * <li> {@link TransactionalTridentKafkaSpout}
 * that consumes sentences from the "test" topic, splits it into words, aggregates
 * and stores the word count in a {@link MemoryMapState}.
 * </li>
 * <li> DRPC query
 * that returns the word counts by querying the trident state (MemoryMapState).
 * </li>
 * </ol>
 * <p>
 *     For more background read the <a href="https://storm.apache.org/documentation/Trident-tutorial.html">trident tutorial</a>,
 *     <a href="https://storm.apache.org/documentation/Trident-state">trident state</a> and
 *     <a href="https://github.com/apache/storm/tree/master/external/storm-kafka"> Storm Kafka </a>.
 * </p>
 */
public class TridentKafkaWordCount implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaWordCount.class);

    private String zkUrl;
    private String brokerUrl;

    TridentKafkaWordCount(String zkUrl, String brokerUrl) {
        this.zkUrl = zkUrl;
        this.brokerUrl = brokerUrl;
    }

    private static TridentKafkaConfig newTridentKafkaConfig(String zkUrl) {
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, "test");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Consume new data from the topic
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return config;
    }


    private Stream addDRPCStream(TridentTopology tridentTopology, TridentState state, LocalDRPC drpc) {
        return tridentTopology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"));
    }

    protected TridentState addTridentState(TridentTopology tridentTopology) {
        // Creates a transactional/opaque kafka spout that consumes any new data published to "test" topic.

        /*final Stream spoutStream = tridentTopology.newStream("spout1",
                new TransactionalTridentKafkaSpout(newTridentKafkaConfig())).parallelismHint(1);*/
        final Stream spoutStream = tridentTopology.newStream("spout1",
                new OpaqueTridentKafkaSpout(newTridentKafkaConfig())).parallelismHint(1);

        return spoutStream.each(spoutStream.getOutputFields(), new Debug(true))
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new DebugMemoryMapState.Factory(), new Count(), new Fields("count"));
    }


    /**
     * Creates a trident topology that consumes sentences from the kafka "test" topic using a
     * {@link TransactionalTridentKafkaSpout} computes the word count and stores it in a {@link MemoryMapState}.
     * A DRPC stream is then created to query the word counts.
     * @param drpc
     * @return
     */
    public StormTopology buildConsumerTopology(LocalDRPC drpc) {
        TridentTopology tridentTopology = new TridentTopology();
        addDRPCStream(tridentTopology, addTridentState(tridentTopology), drpc);
        return tridentTopology.build();
    }

    public Config getConsumerConfig() {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    public Config getProducerConfig() {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setNumWorkers(1);
        return conf;
    }

    /**
     * <p>
     * To run this topology it is required that you have a kafka broker running.
     * </p>
     * Create a topic 'test' with command line,
     * <pre>
     * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
     * </pre>
     * To run in local mode,
     * <pre>
     * storm jar storm-starter-topologies-{version}.jar org.apache.storm.starter.trident.TridentKafkaWordCount
     * </pre>
     * This will also run a local DRPC query and print the word counts.
     * <p>
     * To run in distributed mode, run it with a topology name. You will also need to start a drpc server and
     * specify the drpc server details storm.yaml before submitting the topology.
     * </p>
     * <pre>
     * storm jar storm-starter-topologies-{version}.jar org.apache.storm.starter.trident.TridentKafkaWordCount zkhost:port broker:port wordcount
     * </pre>
     * This will submit two topologies, one for the producer and another for the consumer. You can query the results
     * (word counts) by running an external drpc query against the drpc server.
     */
    public static void main(String[] args) throws Exception {
        final String[] zkBrokerUrl = parseUrl(args);
        run(args, new TridentKafkaWordCount(zkBrokerUrl[0], zkBrokerUrl[1]));
    }

    protected static String[] parseUrl(String[] args) {
        String zkUrl = "localhost:2181";        // the defaults.
        String brokerUrl = "localhost:9092";

        if (args.length > 3 || (args.length == 1 && args[0].matches("^-h|--help$"))) {
            System.out.println("Usage: TridentKafkaWordCount [kafka zookeeper url] [kafka broker url] [topology name]");
            System.out.println("   E.g TridentKafkaWordCount [" + zkUrl + "]" + " [" + brokerUrl + "] [wordcount]");
            System.exit(1);
        } else if (args.length == 1) {
            zkUrl = args[0];
        } else if (args.length == 2) {
            zkUrl = args[0];
            brokerUrl = args[1];
        }

        System.out.println("Using Kafka zookeeper url: " + zkUrl + " broker url: " + brokerUrl);
        return new String[]{zkUrl, brokerUrl};
    }

    protected static void run(String[] args, TridentKafkaWordCount wordCount) throws Exception {
        final String[] zkBrokerUrl = parseUrl(args);

        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        if (args.length == 3)  {
            conf.setNumWorkers(1);

            // submit the PRODUCER topology.
            StormSubmitter.submitTopology(args[2] + "-producer", getProducerConfig(), KafkaProducerTopology.create(brokerUrl, topicName));

            // submit the CONSUMER topology.
            StormSubmitter.submitTopology(args[2] + "-consumer", getConsumerConfig(), TridentKafkaConsumerTopology.newTopology(drpc, tridentSpout));
            TridentKafkaConsumerTopology.submitRemote(args[2] + "-consumer", new TransactionalTridentKafkaSpout(newTridentKafkaConfig(zkBrokerUrl[0])));

        } else {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();

            // submit the CONSUMER topology.
            cluster.submitTopology("wordCounter", wordCount.getConsumerConfig(), wordCount.buildConsumerTopology(drpc));

            // submit the PRODUCER topology.
            Config conf = new Config();
            conf.setMaxSpoutPending(20);
//            conf.setDebug(true);
            cluster.submitTopology("kafkaBolt", conf, wordCount.buildProducerTopology(wordCount.getProducerConfig()));


            IOpaquePartitionedTridentSpout tridentSpout;

            final String prodTpName = null;
            final String brokerUrl;
            final String topicName;

            // submit the PRODUCER topology.
            StormSubmitter.submitTopology(prodTpName, getProducerConfig(), KafkaProducerTopology.create(brokerUrl, topicName));

            final String consTpName = null;
            // submit the CONSUMER topology.
            StormSubmitter.submitTopology(consTpName, getConsumerConfig(), TridentKafkaConsumerTopology.newTopology(drpc, tridentSpout));


            // submit the CONSUMER topology.

            // keep querying the word counts for a minute.
            for (int i = 0; i < 60; i++) {
                LOG.info("--- DRPC RESULT: " + drpc.execute("words", "the and apple snow jumped"));
                System.out.println();
                Thread.sleep(1000);
            }

            cluster.killTopology("kafkaBolt");
            cluster.killTopology("wordCounter");
            cluster.shutdown();
        }

        class Submitter {
            Config config;
            String[] args;

            void submitProducerTopology() {

            }

            void submitConsumerTopology() {

            }

            void submitLocalProducerTopology(String name, StormTopology topology) {
                LocalDRPC drpc = new LocalDRPC();
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(name, conf, topology);
            }

            void submitLocalConsumerTopology() {

            }

            public Config getConsumerConfig() {
                Config conf = new Config();
                conf.setMaxSpoutPending(20);
                conf.setMaxTaskParallelism(1);
                return conf;
            }

            public Config getProducerConfig() {
                Config conf = new Config();
                conf.setMaxSpoutPending(20);
                conf.setNumWorkers(1);
                return conf;
            }
        }
    }
}
