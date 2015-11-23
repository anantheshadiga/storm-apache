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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("Duplicates")
public class StormSqlBolt_3 extends BaseRichBolt {
    protected static final Logger log = LoggerFactory.getLogger(StormSqlBolt_3.class);
    private AtomicInteger ai = new AtomicInteger();

    private OutputCollector collector;

    private Wrapper wrapper;

    public StormSqlBolt_3() {
        wrapper = new Wrapper(new WrapperDependenciesBuilder());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//        wrapper.compileQuery();
    }

    @Override
    public void execute(Tuple input) {
        log.debug("&&&&&&&&&&&&&&&&&& DEBUG &&&&&&&&&&&&&&&&&& ");
        if (input.getSourceComponent().equals(StormSqlTopology.STORM_SQL_BOLT)) {
            log.info("++++++++ RECEIVED FILTERED TUPLE: [{}]", input);
        } else {
            log.info("++++++++ RECEIVED TUPLE: [{}]", input);
        }

        if (wrapper.eval(input)) {
            log.info("++++++++ Evaluated to TRUE");
            wrapper.execute(input, collector);
        } else {
            log.info("++++++++ Evaluated to FALSE");
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("F2", "F1", "F3"));
    }

    private void sleep(long time) {
        try {
            log.info("&&&&&&&&&& Sleeping Starting [{}]", ai.get());
            Thread.sleep(time);
            log.info("&&&&&&&&&& Sleeping Finished [{}]", ai.incrementAndGet());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
