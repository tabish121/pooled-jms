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

/**
 * An interface type that represents the owner of a session proxy who should be
 * signaled when a session proxy has specific events in its life-cylce triggered.
 *
 * @param <SP> the actual session proxy type that is managed by this instance
 */
public interface JmsPoolSessionProxyManager<SP extends JmsPoolAbstractSessionProxy<SP>> {

    /**
     * Should be called from the child session proxy when it has been released by its
     * owner via a call to the session close method.
     *
     * @param session
     * 		The JMS session proxy that is managed by this instance.
     *
     * @return the JMS session proxy instance that was passed
     */
    SP releaseSession(SP session);

    /**
     * Should be called from the child session proxy when its owner decides that some
     * critical error has occurred and it should be closed and removed from the pool of
     * JMS sessions.
     *
     * @param session
     * 		The JMS session proxy that is managed by this instance.
     *
     * @return the JMS session proxy instance that was passed
     */
    SP closeSession(SP session);

}
