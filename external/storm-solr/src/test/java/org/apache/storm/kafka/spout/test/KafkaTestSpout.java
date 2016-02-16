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
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.strategy.KafkaSpoutStrategy;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Random;

public class KafkaTestSpout extends KafkaSpout {
    public static final List<Values> listValues = Lists.newArrayList(
            getValues("1"), getValues("2"), getValues("3"));


    public KafkaTestSpout() {
        this(null);
    }

    public KafkaTestSpout(KafkaSpoutStrategy kafkaSpoutStrategy) {
        super(kafkaSpoutStrategy);
    }

    private static Values getValues(String suf) {
        String suffix = "hmcl_" + suf;
        return new Values(suffix);
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        final Values values = listValues.get(rand.nextInt(listValues.size()));
        collector.emit(values, values.get(0));
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }
}
