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

package org.apache.storm.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OffsetAndMetadataStub {
    public static final String COMMIT_METADATA = "{\"topologyId\":\"tp1\",\"taskId\":3,\"threadName\":\"Thread-20\"}";

    public static OffsetAndMetadata committed(KafkaConsumer<String, String> consumerMock) throws java.io.IOException {
        OffsetAndMetadata oam = mock(OffsetAndMetadata.class);

        when(consumerMock.committed(any(TopicPartition.class)))
            .thenReturn(oam);

        when(oam.metadata())
            .thenReturn(COMMIT_METADATA);

        return oam;
    }
}
