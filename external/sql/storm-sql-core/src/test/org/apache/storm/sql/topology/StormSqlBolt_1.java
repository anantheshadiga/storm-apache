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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("Duplicates")
public class StormSqlBolt_1 extends BaseRichBolt {
    protected static final Logger log = LoggerFactory.getLogger(StormSqlBolt_1.class);
    private AtomicInteger ai = new AtomicInteger();

    private OutputCollector collector;

    private ChannelContext ctx;
    private ChannelHandler handler;
    private DataSourcesProvider dataSourceProvider;
    private DataSource dataSource;
    private Wrapper wrapper;

    public StormSqlBolt_1() {
        wrapper = new Wrapper();
        dataSource = new MyDataSource();    // sets ctx
        ctx = ((MyDataSource)dataSource).getCtx();
        dataSourceProvider = new MyDataSourcesProvider(dataSource);
        handler = new MyChannnelHandler(null);          //TODO
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        prepareQuery();
    }

    private void prepareQuery() {
        try {
            DataSourcesRegistry.providerMap().put("RBTS", dataSourceProvider);      //RBTS - Rules Bolt Table Schema

            List<String> stmnt = new ArrayList<>();
            stmnt.add("CREATE EXTERNAL TABLE RBT (F1 INTEGER, F2 INT, F3 INT) LOCATION 'RBTS:///RBT'");
            stmnt.add("SELECT F1,F2,F3 FROM RBT WHERE F1 < 2 AND F2 < 3 AND F3 < 4");
            StormSql stormSql = StormSql.construct();
            stormSql.execute(stmnt, handler);
        } catch (Exception e) {
            throw new RuntimeException("Failed preparing query", e);
        }

    }

    @Override
    public void execute(Tuple input) {
        log.debug("&&&&&&&&&&&&&&&&&& DEBUG &&&&&&&&&&&&&&&&&& ");
        if (input.getSourceComponent().equals(StormSqlTopology.STORM_SQL_BOLT)) {
            log.info("++++++++ RECEIVED FILTERED TUPLE: [{}]", input);
        } else {
            log.info("++++++++ RECEIVED TUPLE: [{}]", input);
        }

//        sleep(2000);
        Values values = createValues(input);
        ctx.emit(values);
        //TODO evaluated
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

    // =========== Storm SQL Code ==============

    class Wrapper {

    }

    class MyDataSource implements DataSource {
        ChannelContext ctx;
        @Override
        public void open(ChannelContext ctx) {
            this.ctx = new SpecialChannelContext(ctx, null);    //TODO
        }

        public ChannelContext getCtx() {
            return ctx;
        }
    }

    class SpecialChannelContext implements ChannelContext {
        ChannelContext parent;
        private ChannelHandler channelHandler;

        public SpecialChannelContext(ChannelContext parent, ChannelHandler channelHandler) {
            this.parent = parent;
            this.channelHandler = channelHandler;
        }

        @Override
        public void emit(Values data) {
            parent.emit(data);
        }

        @Override
        public void fireChannelInactive() {

        }
    }

    // ==========

    class MyChannnelHandler implements ChannelHandler {
        ChannelContext channelContext;

        public MyChannnelHandler(ChannelContext channelContext) {
            this.channelContext = channelContext;
        }

        @Override
        public void dataReceived(ChannelContext ctx, Values data) {
            log.info("++++++++ Data Received {}", data);
//            buffer.add(data);
            collector.emit(data);
        }

        @Override
        public void channelInactive(ChannelContext ctx) {

        }

        @Override
        public void exceptionCaught(Throwable cause) {

        }
    }

    // ==========

    class MyDataSourcesProvider implements DataSourcesProvider {
        DataSource dataSource;

        public MyDataSourcesProvider(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public String scheme() {
            return "RBTS";
        }

        @Override
        public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass, List<Map.Entry<String, Class<?>>> fields) {
            return dataSource;
        }

        public DataSource getDataSource() {
            return dataSource;
        }
    }
}
