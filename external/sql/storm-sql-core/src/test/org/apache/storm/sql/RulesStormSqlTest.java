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

package org.apache.storm.sql;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RulesStormSqlTest {
    @Test
    public void testName() throws Exception {
        DataSourcesRegistry.providerMap().put(RulesDataSourcesProvider.RULES_SCHEME, new RulesDataSourcesProvider());

        List<String> stmnt = new ArrayList<>();
        stmnt.add("CREATE EXTERNAL TABLE RULES_SCHEME(F1 INT, F2 INT, F3 INT) LOCATION ' ??? mock:///foo'");
        stmnt.add("SELECT F1,F2,F3 FROM RULES_SCHEME WHERE F1 < 2 AND F2 > 3 AND F3 < 4");
        StormSql stormSql = StormSql.construct();
        stormSql.execute(stmnt, new RulesChannelHandler());
    }

    public class RulesDataSourcesProvider implements DataSourcesProvider {
        public static final String RULES_SCHEME = "RULES_SCHEME";

        @Override
        public String scheme() {
            return RULES_SCHEME;
        }

        @Override
        public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass, List<Map.Entry<String, Class<?>>> fields) {
            return null;
        }
    }

    public class RulesDataSource implements DataSource {
        @Override
        public void open(ChannelContext ctx) {
//            ctx.emit();
        }
    }

    public class RulesChannelContext implements ChannelContext {
        @Override
        public void emit(Values data) {

        }

        @Override
        public void fireChannelInactive() {
            // call when the bolt is garbage collected?
        }
    }

    public class RulesChannelHandler implements ChannelHandler {
        @Override
        public void dataReceived(ChannelContext ctx, Values data) {

        }

        @Override
        public void channelInactive(ChannelContext ctx) {

        }

        @Override
        public void exceptionCaught(Throwable cause) {

        }

        class DatasourceBolt implements IBolt, DataSource {
            private ChannelContext ctx;
            private final List<List<Object>> buffer = new ArrayList<>();

            private ChannelHandler handler =  new ChannelHandler() {
                @Override
                public void dataReceived(ChannelContext ctx, Values data) {
                    buffer.add(data);
                }

                @Override
                public void channelInactive(ChannelContext ctx) {

                }

                @Override
                public void exceptionCaught(Throwable cause) {

                }
            };

            @Override
            public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
                try {
                    StormSql sql = StormSql.construct();
                    sql.execute(new ArrayList<String>(){{add("SELECT xxx");}}, handler);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void execute(Tuple input) {
                ctx.emit(new Values(input));
                if (!buffer.isEmpty()) {
                    // next stage
                }
            }

            @Override
            public void cleanup() {

            }

            @Override
            public void open(ChannelContext ctx) {
                this.ctx = ctx;
            }
        }


        //        List<Map.Entry<String, Class<?>>> fields = new ArrayList<>();   //TODO
        //        DataSourcesRegistry.construct(null, null, null, fields);


    }

}
