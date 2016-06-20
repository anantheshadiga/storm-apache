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

package org.apache.storm.kafka.spout.trident;

import org.apache.kafka.common.TopicPartition;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MyCoordinator<K,V> implements IOpaquePartitionedTridentSpout.Coordinator<List<TopicPartition>>, Serializable {
    private KafkaManager<K,V> kafkaManager;

    public MyCoordinator(KafkaManager<K, V> kafkaManager) {
        this.kafkaManager = kafkaManager;
    }

    @Override
    public boolean isReady(long txid) {
        return true;    // the "old" trident kafka spout is like this
    }

    @Override
    public List<TopicPartition> getPartitionsForBatch() {
        return new ArrayList<>(kafkaManager.getTopicPartitions());
    }

    @Override
    public void close() {
        // the "old" trident kafka spout is like this
    }
}
