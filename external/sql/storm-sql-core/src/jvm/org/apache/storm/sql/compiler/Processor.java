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

// GENERATED CODE
package org.apache.storm.sql.compiler;

import backtype.storm.tuple.Values;
import org.apache.storm.sql.runtime.AbstractChannelHandler;
import org.apache.storm.sql.runtime.AbstractValuesProcessor;
import org.apache.storm.sql.runtime.ChannelContext;
import org.apache.storm.sql.runtime.ChannelHandler;
import org.apache.storm.sql.runtime.Channels;
import org.apache.storm.sql.runtime.DataSource;

import java.util.Map;

public final class Processor extends AbstractValuesProcessor {
    private static final ChannelHandler ENUMERABLETABLESCAN_3 =
            new AbstractChannelHandler() {
                @Override
                public void dataReceived(ChannelContext ctx, Values _data) {
                    ctx.emit(_data);
                }
            };

    private static final ChannelHandler LOGICALFILTER_4 =
            new AbstractChannelHandler() {
                @Override
                public void dataReceived(ChannelContext ctx, Values _data) {
                    final boolean t1;
                    final boolean t2;
                    int t3 = (int)(_data.get(0));
                    if (false) {}
                    else { t2 = t3 < 2; }
                    if (!(t2)) { t1 = false; }
                    else {
                        final boolean t4;
                        int t5 = (int)(_data.get(1));
                        if (false) {}
                        else { t4 = t5 < 3; }
                        t1 = t4;
                    }
                    if (t1) {
                        ctx.emit(_data);
                    }
                }
            };

    private static final ChannelHandler LOGICALPROJECT_5 =
            new AbstractChannelHandler() {
                @Override
                public void dataReceived(ChannelContext ctx, Values _data) {
                    int t1 = (int)(_data.get(0));
                    int t2 = (int)(_data.get(1));
                    int t3 = (int)(_data.get(2));
                    ctx.emit(new Values(t1,t2,t3));
                }
            };

    @Override
    public void initialize(Map<String, DataSource> data,
                           ChannelHandler result) {
        ChannelContext r = Channels.chain(Channels.voidContext(), result);
        ChannelContext CTX_5 = Channels.chain(r, LOGICALPROJECT_5);
        ChannelContext CTX_4 = Channels.chain(CTX_5, LOGICALFILTER_4);
        ChannelContext CTX_3 = Channels.chain(CTX_4, ENUMERABLETABLESCAN_3);
        if (!data.containsKey("RBT"))
            throw new RuntimeException("Cannot find table " + "RBT");
        data.get("RBT").open(CTX_3);
    }
}