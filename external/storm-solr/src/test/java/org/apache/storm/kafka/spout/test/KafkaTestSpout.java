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

import org.apache.storm.kafka.spout.LoggerHugo;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class KafkaTestSpout extends BaseRichSpout {
    public static final List<Values> listValues = Lists.newArrayList(
            getValues("1"), getValues("2"), getValues("3"));
    private SpoutOutputCollector collector;


    public KafkaTestSpout() {

    }

    private static Values getValues(String suf) {
        String suffix = "hmcl_" + suf;
        return new Values(suffix);
    }

    int count = 0;

    @Override
    public void nextTuple() {
        LoggerHugo.doLog("count start = " + count);
        final Random rand = new Random();
        final Values values = listValues.get(rand.nextInt(listValues.size()));
        collector.emit(values, values.get(0));
//        Thread.yield();
        LoggerHugo.doLog("count end = " +  ++count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
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

    @Override
    public void activate() {
        //resume processing
    }

    @Override
    public void deactivate() {
        //commit
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {
        //remove resources
    }

    public Fields getOutputFields() {
        return new Fields("hmcl-kafka-test-spout");
    }
}
