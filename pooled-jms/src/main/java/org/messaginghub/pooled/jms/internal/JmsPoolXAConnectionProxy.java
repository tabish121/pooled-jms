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

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.XAConnection;
import jakarta.jms.XAQueueConnection;
import jakarta.jms.XASession;
import jakarta.jms.XATopicConnection;

/**
 * An XA-aware shared connection from the pool. When a session is created and an XA transaction
 * is active, the session will automatically be enlisted in the current transaction.
 */
public class JmsPoolXAConnectionProxy extends JmsPoolConnectionProxy implements XAConnection, XATopicConnection, XAQueueConnection{

    private final XAConnection connection;

    public JmsPoolXAConnectionProxy(JmsPoolConnectionConfiguration configuration, XAConnection connection) {
        super(configuration, connection);

        this.connection = connection;
    }

    @Override
    protected XASession makeSession(JmsPoolSessionKey key) throws JMSException {
        return (XASession) connection.createSession(key.isTransacted(), key.getAckMode());
    }

    @Override
    protected JmsPoolXASessionProxy makeSessionProxy(JmsPoolConnectionProxy connection, JmsPoolSessionKey sessionKey, Session session,
                                                     GenericKeyedObjectPool<JmsPoolSessionKey, JmsPoolSessionProxy> sessionPool) throws JMSException {
        return new JmsPoolXASessionProxy((JmsPoolXAConnectionProxy) connection, sessionKey, makeSession(sessionKey), sessionPool);
    }

    @Override
    public JmsPoolXASessionProxy createSession(boolean transacted, int sessionMode) throws JMSException {
        return (JmsPoolXASessionProxy) super.createSession(transacted, sessionMode);
    }

    @Override
    public JmsPoolXASessionProxy createXASession() throws JMSException {
        return (JmsPoolXASessionProxy) super.createSession(true, Session.SESSION_TRANSACTED); // TODO params ?
    }

    @Override
    public JmsPoolXASessionProxy createXATopicSession() throws JMSException {
        return createXASession();
    }

    @Override
    public JmsPoolXASessionProxy createXAQueueSession() throws JMSException {
        return createXASession();
    }
}
