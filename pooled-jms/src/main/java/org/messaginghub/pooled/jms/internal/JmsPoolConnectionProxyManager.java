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

import jakarta.jms.JMSException;

/**
 * An interface type that represents the owner of a connection proxy who should be
 * signaled when a connection proxy has specific events in its life-cylce triggered.
 *
 * @param <CP> the actual connection proxy type that is managed by this instance
 */
public interface JmsPoolConnectionProxyManager<CP extends JmsPoolAbstractConnectionProxy<CP, ?>> {

    /**
     * Called from the owned connection proxy when an additional reference is requested
     *
     * @param connection
     * 		The connection proxy instance invoking this method
     *
     * @return the connection proxy instance invoking this method
     *
     * @throws JMSException if the connection is already unreferenced and cannot give a new reference
     */
    CP tryAcquireReference(CP connection) throws JMSException;

    /**
     * Called from the owned connection proxy when a reference is being released
     *
     * @param connection
     * 		The connection proxy instance invoking this method
     *
     * @return the connection proxy instance invoking this method
     */
    CP tryReleaseReference(CP connection);

    /**
     * Called from the owned connection proxy when the connection is being treated as failed
     * and it should be closed and all its resources also closed.
     *
     * @param connection
     * 		The connection proxy instance invoking this method
     *
     * @return the connection proxy instance invoking this method
     */
    CP invalidateConnection(CP connection);

}
