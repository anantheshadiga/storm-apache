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
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
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

public class TridentKafkaConsumerTopology {

    public static void submitLocal(String name, IOpaquePartitionedTridentSpout tridentSpout) {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, newConsumerConfig(), newTopology(drpc, tridentSpout));
    }

    public static void submitRemote(String name, IOpaquePartitionedTridentSpout tridentSpout) {
        try {
            StormSubmitter.submitTopology(name, newConsumerConfig(), newTopology(null, tridentSpout));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * See {@link TridentKafkaConsumerTopology#newTopology(LocalDRPC, IOpaquePartitionedTridentSpout)}
     */
    public static StormTopology newTopology(IOpaquePartitionedTridentSpout tridentSpout) {
        return newTopology(null, tridentSpout);
    }

    /**
     * @param drpc The DRPC stream to be used in querying the word counts. Can be null in distributed mode
     * @return a trident topology that consumes sentences from the kafka topic specified using a
     * {@link TransactionalTridentKafkaSpout} computes the word count and stores it in a {@link MemoryMapState}.
     */
    public static StormTopology newTopology(LocalDRPC drpc, IOpaquePartitionedTridentSpout tridentSpout) {
        final TridentTopology tridentTopology = new TridentTopology();
        addDRPCStream(tridentTopology, addTridentState(tridentTopology, tridentSpout), drpc);
        return tridentTopology.build();
    }

    private static Stream addDRPCStream(TridentTopology tridentTopology, TridentState state, LocalDRPC drpc) {
        return tridentTopology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"));
    }

    private static TridentState addTridentState(TridentTopology tridentTopology, IOpaquePartitionedTridentSpout tridentSpout) {
        final Stream spoutStream = tridentTopology.newStream("spout1", tridentSpout).parallelismHint(1);

        return spoutStream.each(spoutStream.getOutputFields(), new Debug(true))
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new DebugMemoryMapState.Factory(), new Count(), new Fields("count"));
    }

    private static Config newConsumerConfig() {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setMaxTaskParallelism(1);
        return conf;
    }
    
}
