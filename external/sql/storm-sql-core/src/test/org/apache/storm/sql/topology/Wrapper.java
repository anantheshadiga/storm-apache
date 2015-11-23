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
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.sql.DataSourcesProvider;
import org.apache.storm.sql.DataSourcesRegistry;
import org.apache.storm.sql.StormSql;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Wrapper implements Serializable {
    private DataSource dataSource;  // step 1
    private ChannelContext channelContext;     // step 2 - Data Source sets context
    private ChannelHandler channelHandler; // step 3
    private DataSourcesProvider dataSourceProvider; // step 4
    private boolean evaluates;

    public Wrapper() {
        // These steps cannot be changed
        this.dataSource = this.new MyDataSource();
        this.channelHandler = this.new MyChannelHandler();
        this.dataSourceProvider = this.new MyDataSourcesProvider();
        compileQuery();
    }

    public boolean eval(Tuple input) {
        Values values = createValues(input);
        channelContext.emit(values);
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

    private void compileQuery() {
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

    class MyDataSource implements DataSource {
        @Override
        public void open(ChannelContext ctx) {
            Wrapper.this.channelContext = ctx;
        }
    }

    class MyChannelHandler implements ChannelHandler {
        @Override
        public void dataReceived(ChannelContext ctx, Values data) {
            StormSqlBolt_3.log.info("++++++++ Data Received {}", data);
            Wrapper.this.evaluates = true;
        }

        @Override
        public void channelInactive(ChannelContext ctx) { }

        @Override
        public void exceptionCaught(Throwable cause) { }
    }

    class MyDataSourcesProvider implements DataSourcesProvider {
        @Override
        public String scheme() {
            return "RBTS";
        }

        @Override
        public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass, List<Map.Entry<String, Class<?>>> fields) {
            return Wrapper.this.dataSource;
        }
    }
}
