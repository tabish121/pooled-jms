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

import javax.transaction.xa.XAResource;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import jakarta.jms.JMSException;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicSession;
import jakarta.jms.XAQueueSession;
import jakarta.jms.XASession;
import jakarta.jms.XATopicSession;

public class JmsPoolXASessionProxy extends JmsPoolSessionProxy implements XASession, XATopicSession, XAQueueSession {

    private final XASession session;

    JmsPoolXASessionProxy(JmsPoolXAConnectionProxy connection, JmsPoolSessionKey key, XASession session, GenericKeyedObjectPool<JmsPoolSessionKey, JmsPoolSessionProxy> sessionPool) {
        super(connection, key, session, sessionPool);

        this.session = session;
    }

    @Override
    public QueueSession getQueueSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public TopicSession getTopicSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public Session getSession() throws JMSException {
        checkClosed();
        return this;
    }

    @Override
    public XAResource getXAResource() {
        checkClosedRuntimeEx();
        return session.getXAResource();
    }
}
