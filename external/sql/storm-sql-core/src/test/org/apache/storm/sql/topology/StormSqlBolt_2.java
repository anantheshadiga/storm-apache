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

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("Duplicates")
public class StormSqlBolt_2 extends BaseRichBolt {
    protected static final Logger log = LoggerFactory.getLogger(StormSqlBolt_2.class);
    private AtomicInteger ai = new AtomicInteger();

    private OutputCollector collector;

    private Wrapper wrapper;

    public StormSqlBolt_2() {
        wrapper = new Wrapper();
        // Step 1 && Step 2
        MyDataSource dataSource = new MyDataSource(wrapper);
        wrapper.setDataSource(dataSource);
        MyChannelHandler channelHandler = new MyChannelHandler(wrapper);
        // Step 3
        wrapper.setChannelHandler(channelHandler);
        // Step 4
        wrapper.setDataSourceProvider(new MyDataSourcesProvider(dataSource));
        // Step 5
        wrapper.compileQuery();
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

    class Wrapper implements Serializable {
        private DataSource dataSource;  // step 1
        private ChannelContext ctx;     // step 2 - Data Source sets context
        private ChannelHandler channelHandler; // step 3
        private DataSourcesProvider dataSourceProvider; // step 4
        private boolean evaluates;

        public Wrapper() {
        }

        public boolean eval(Tuple input) {
            Values values = createValues(input);
            ctx.emit(values);
            boolean evals = evaluates;      // This value gets set synchronously in a callback in ChannelHandler
            evaluates = false;              // reset
            return evals;
        }

        public void execute(Tuple tuple, OutputCollector outputCollector) {
            outputCollector.emit(tuple, createValues(tuple));
        }

        private Values createValues(Tuple input) {
            return new Values(input.getInteger(0), input.getInteger(1), input.getInteger(2));
        }

        public void compileQuery() {
            try {
                DataSourcesRegistry.providerMap().put("RBTS", dataSourceProvider);      //RBTS - Rules Bolt Table Schema
                List<String> stmnt = new ArrayList<>();
                stmnt.add("CREATE EXTERNAL TABLE RBT (F1 INTEGER, F2 INT, F3 INT) LOCATION 'RBTS:///RBT'");
                stmnt.add("SELECT F1,F2,F3 FROM RBT WHERE F1 < 2 AND F2 < 3 AND F3 < 4");
                StormSql stormSql = StormSql.construct();
                stormSql.execute(stmnt, channelHandler);
            } catch (Exception e) {
                throw new RuntimeException("Failed preparing query", e);
            }
        }

        public void setCtx(ChannelContext ctx) {
            this.ctx = ctx;
        }

        public ChannelContext getCtx() {
            return ctx;
        }

        public void setDataSource(DataSource dataSource) {
            this.dataSource = dataSource;           // sets ctx
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public DataSourcesProvider getDataSourceProvider() {
            return dataSourceProvider;
        }

        public void setDataSourceProvider(DataSourcesProvider dataSourceProvider) {
            this.dataSourceProvider = dataSourceProvider;
        }

        public ChannelHandler getChannelHandler() {
            return channelHandler;
        }

        public void setChannelHandler(ChannelHandler channelHandler) {
            this.channelHandler = channelHandler;
        }

        public void setEvaluates(boolean evaluates) {
            this.evaluates = evaluates;
        }

        public boolean isEvaluates() {
            return evaluates;
        }
    }

    class MyDataSource implements DataSource {
        private Wrapper wrapper;

        public MyDataSource(Wrapper wrapper) {
            this.wrapper = wrapper;
        }

        @Override
        public void open(ChannelContext ctx) {
            wrapper.setCtx(ctx);
        }
    }

    // ==========

    class MyChannelHandler implements ChannelHandler {
        private Wrapper wrapper;

        public MyChannelHandler(Wrapper wrapper) {
            this.wrapper = wrapper;
        }

        @Override
        public void dataReceived(ChannelContext ctx, Values data) {
            log.info("++++++++ Data Received {}", data);
            wrapper.setEvaluates(true);
        }

        @Override
        public void channelInactive(ChannelContext ctx) { }

        @Override
        public void exceptionCaught(Throwable cause) { }
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
