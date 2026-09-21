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
package org.messaginghub.pooled.jms;

import org.messaginghub.pooled.jms.internal.JmsPoolConnectionProxy;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicSession;

/**
 * Represents a proxy {@link Connection} which implements both the {@link TopicConnection} and
 * {@link QueueConnection} interfaces and on a call to {@link #close()} will return its reference
 * to the pooled connection back to JMS connection pool manager.
 * <p>
 * <b>NOTE</b> this implementation is only intended for use when sending messages. It does not deal
 * with pooling of consumers but it can be used to create consumers as normal.
 */
public class JmsPoolConnection extends JmsPoolAbstractConnection {

    /**
     * Creates a new JMS pool connection instance that uses the given connection proxy to
     * create and manage its JMS resources. The connection proxy instance can be shared
     * amongst many instances of the JMS pool connection wrapper.
     *
     * @param connection
     *      The Connection proxy that wraps a connection created by the provider library.
     */
    public JmsPoolConnection(JmsPoolConnectionProxy connection) {
        super(connection);
    }

    @Override
    protected Session doCreateSession(boolean transacted, int ackMode) throws JMSException {
        return afterSessionCreated(new JmsPoolSession(safeGetConnection().createSession(transacted, ackMode), transacted));
    }

    @Override
    protected TopicSession doCreateTopicSession(boolean transacted, int ackMode) throws JMSException {
        return afterSessionCreated(new JmsPoolSession(safeGetConnection().createSession(transacted, ackMode), transacted));
    }

    @Override
    protected QueueSession doCreateQueueSession(boolean transacted, int ackMode) throws JMSException {
        return afterSessionCreated(new JmsPoolSession(safeGetConnection().createSession(transacted, ackMode), transacted));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + connection + " }";
    }

    @Override
    protected JmsPoolConnectionProxy safeGetConnection() throws JMSException {
        checkClosed();
        return (JmsPoolConnectionProxy) this.connection;
    }
}
