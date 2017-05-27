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

package org.apache.storm.kafka.spout.trident;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

/**
 * Exception thrown by implementations of {@link ConsumerRebalanceListener} and that serves as a mechanism to
 * fail transactions. In face of Kafka consumer rebalance, the cleanest and predictable way to handle state change
 * is to fail transaction and re-send it.
 */
public class KafkaConsumerRebalanceTransactionAbortException extends RuntimeException {
    public KafkaConsumerRebalanceTransactionAbortException() {
        super();
    }

    public KafkaConsumerRebalanceTransactionAbortException(String message) {
        super(message);
    }

    public KafkaConsumerRebalanceTransactionAbortException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConsumerRebalanceTransactionAbortException(Throwable cause) {
        super(cause);
    }

    protected KafkaConsumerRebalanceTransactionAbortException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
