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

package org.apache.storm.sql.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class StormSqlTopology {
    protected static final String STORM_SQL_SPOUT = "STORM_SQL_SPOUT";
    protected static final String STORM_SQL_BOLT = "STORM_SQL_BOLT";
    protected static final String STORM_SQL_FILTERED_BOLT = "STORM_SQL_FILTERED_BOLT";

    public static void main(String[] args) throws Exception {
        testTopology();
    }

//    @Test
    public static void testTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(STORM_SQL_SPOUT, new StormSqlSpout());
        builder.setBolt(STORM_SQL_BOLT, new StormSqlBolt_3()).shuffleGrouping(STORM_SQL_SPOUT);
//        builder.setBolt(STORM_SQL_BOLT, new StormSqlBolt()).shuffleGrouping(STORM_SQL_SPOUT);
        builder.setBolt(STORM_SQL_FILTERED_BOLT, new StormSqlFilteredBolt()).shuffleGrouping(STORM_SQL_BOLT);

        StormTopology sqlTopology = builder.createTopology();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("STORM_SQL_TOPOLOGY", getConfig(), sqlTopology);
    }

    private static Config getConfig() {
        final Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(1);
        config.setNumWorkers(1);
        return config;
    }
}