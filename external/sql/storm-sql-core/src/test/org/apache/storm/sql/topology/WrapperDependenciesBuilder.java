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

import org.apache.storm.sql.DataSourcesProvider;
import org.apache.storm.sql.DataSourcesRegistry;
import org.apache.storm.sql.StormSql;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.DataSource;

import java.util.ArrayList;
import java.util.List;

public class WrapperDependenciesBuilder {
    private ChannelHandler channelHandler;

    public DataSource getDataSource() {
        return new Wrapper.MyDataSource();
    }

    public ChannelHandler getChannelHandler() {     // Sets Wrapper.this.channelContext
        channelHandler = new Wrapper.MyChannelHandler();
        return channelHandler;
    }

    public DataSourcesProvider getDataSourceProvider() {
        Wrapper.MyDataSourcesProvider dataSourcesProvider = new Wrapper.MyDataSourcesProvider();
        compileQuery(dataSourcesProvider);
        return dataSourcesProvider;
    }

    private void compileQuery(Wrapper.MyDataSourcesProvider dataSourcesProvider) {
        try {
            DataSourcesRegistry.providerMap().put("RBTS", dataSourcesProvider);      //RBTS - Rules Bolt Table Schema
            List<String> stmnt = new ArrayList<>();
            stmnt.add("CREATE EXTERNAL TABLE RBT (F1 INTEGER, F2 INT, F3 INT) LOCATION 'RBTS:///RBT'");
            stmnt.add("SELECT F1,F2,F3 FROM RBT WHERE F1 < 2 AND F2 < 3 AND F3 < 4");
            StormSql stormSql = StormSql.construct();
            stormSql.execute(stmnt, channelHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile query", e);
        }
    }
}
