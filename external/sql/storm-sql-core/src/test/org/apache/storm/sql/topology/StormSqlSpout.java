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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class StormSqlSpout extends BaseRichSpout {
    protected static final Logger log = LoggerFactory.getLogger(StormSqlSpout.class);

    private SpoutOutputCollector collector;

    private static final List<Values> LIST_VALUES = Lists.newArrayList(new Values(1,2,3), new Values(4,5,6));

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        final Random rand = new Random();
        final Values values = LIST_VALUES.get(rand.nextInt(LIST_VALUES.size()));
        log.debug("++++++++ Emitting Tuple: [{}]", values);
        collector.emit(values);
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        log.debug("++++++++ DECLARING OUTPUT FIELDS");
        declarer.declare(getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields("F1", "F2", "F3");
    }

    @Override
    public void close() {
        super.close();
    }
}
