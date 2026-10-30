/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms.internal;

import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;

/**
 * An interface type that represents the owner of a JMS MessageProducer proxy who should be
 * signaled when a MessageProducer proxy has specific events in its life-cylce triggered.
 */
public interface JmsMessageProducerProxyManager {

    /**
     * Called from the owned producer proxy when a reference is being released such as when
     * the wrapper JMS MessageProducer is closed by the client.
     *
     * @param producer
     * 		The producer proxy instance invoking this method
     * @param destination
     * 		The destination that the producer proxy is assigned
     *
     * @return the producer proxy instance invoking this method
     */
    JmsMessageProducerProxy tryReleaseReference(JmsMessageProducerProxy producer, Destination destination);

    /**
     * Called from the owned producer proxy when the producer is being treated as failed
     * and its provider supplied {@link MessageProducer} should be closed.
     *
     * @param producer
     * 		The producer proxy instance invoking this method
     * @param destination
     * 		The destination that the producer proxy is assigned
     *
     * @return the producer proxy instance invoking this method
     */
    JmsMessageProducerProxy destroyProducer(JmsMessageProducerProxy producer, Destination destination);

}
