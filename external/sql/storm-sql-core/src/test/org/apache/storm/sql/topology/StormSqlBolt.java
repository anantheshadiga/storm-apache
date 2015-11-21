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
import backtype.storm.tuple.Values;
import org.apache.storm.sql.DataSourcesProvider;
import org.apache.storm.sql.DataSourcesRegistry;
import org.apache.storm.sql.StormSql;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class StormSqlBolt extends BaseRichBolt implements DataSource {
    protected static final Logger log = LoggerFactory.getLogger(StormSqlBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        prepareQuery();
    }

    private void prepareQuery() {
        try {
            DataSourcesRegistry.providerMap().put("RBTS", dataSourceProvider);      //RBTS - Rules Bolt Table Schema

            List<String> stmnt = new ArrayList<>();
            stmnt.add("CREATE EXTERNAL TABLE RBT (F1 INT, F2 INT, F3 INT) LOCATION 'RBTS:///RBT'");
            stmnt.add("SELECT F1,F2,F3 FROM RBT WHERE F1 < 2 AND F2 < 3 AND F3 < 4");
            StormSql stormSql = StormSql.construct();
            stormSql.execute(stmnt, handler);
        } catch (Exception e) {
            throw new RuntimeException("Failed preparing query", e);
        }

    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(StormSqlTopology.STORM_SQL_BOLT)) {
            log.info("++++++++ RECEIVED FILTERED TUPLE: [{}]", input);
        } else {
            log.info("++++++++ RECEIVED TUPLE: [{}]", input);
        }

        Values values = createValues(input);
        ctx.emit(values);
        log.info("++++++++ EMITTED VALUES: [{}]", input);
        /*if (!buffer.isEmpty()) {
            log.info("++++++++ Contents of buffer {}", buffer);
            collector.emit(input, values);
            buffer.remove();
        }*/
        collector.ack(input);
    }

    private Values createValues(Tuple input) {
        return new Values(input.getInteger(0), input.getInteger(1), input.getInteger(2));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("F1", "F2", "F3"));
    }

    // =========== Storm SQL Code ==============

    private ChannelContext ctx;

    @Override
    public void open(ChannelContext ctx) {
        this.ctx = ctx;
    }

    // ==========

    private final Queue<List<Object>> buffer = new LinkedList<>();

    private ChannelHandler handler =  new ChannelHandler() {
        @Override
        public void dataReceived(ChannelContext ctx, Values data) {
//            log.info("++++++++ Data Received {}", data);
//            buffer.add(data);
            collector.emit(data);
        }

        @Override
        public void channelInactive(ChannelContext ctx) {

        }

        @Override
        public void exceptionCaught(Throwable cause) {

        }
    };

    // ==========

    private DataSourcesProvider dataSourceProvider = new DataSourcesProvider() {
        @Override
        public String scheme() {
            return "RBTS";
        }

        @Override
        public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass, List<Map.Entry<String, Class<?>>> fields) {
            return StormSqlBolt.this;
        }
    };
}
