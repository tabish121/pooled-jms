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
import jakarta.jms.Session;
import jakarta.jms.XAConnection;
import jakarta.jms.XASession;

/**
 * An XA-aware shared connection from the pool. When a session is created and an XA transaction
 * is active, the session will automatically be enlisted in the current transaction.
 */
public class JmsXAConnectionProxy extends JmsAbstractConnectionProxy<JmsXAConnectionProxy, JmsXASessionProxy> implements XAConnection {

    public JmsXAConnectionProxy(JmsConnectionProxyConfiguration configuration, XAConnection connection,
                                    JmsConnectionProxyManager<JmsXAConnectionProxy> manager) {
        super(configuration, connection, manager);
    }

    @Override
    public XASession createXASession() throws JMSException {
        return doCreateSession(true, Session.SESSION_TRANSACTED);  // TODO what are the correct parameters?
    }

    @Override
    protected JmsXAConnectionProxy self() {
        return this;
    }

    @Override
    XAConnection getConnection() {
        return (XAConnection) connection;
    }

    @Override
    protected JmsPoolXASessionsPool createSessionPool(JmsConnectionProxyConfiguration configuration) {
        return new JmsPoolXASessionsPool(configuration);
    }

    private class JmsPoolXASessionsPool extends JmsAbstractSessionPool<JmsXASessionProxy> {

        JmsPoolXASessionsPool(JmsConnectionProxyConfiguration configuration) {
            super(configuration);
        }

        @Override
        protected JmsXASessionProxy createSessionProxy(boolean transacted, int sessionMode,
                                                           JmsSessionProxyManager<JmsXASessionProxy> manager) throws JMSException {
            final XASession session = (XASession) getConnection().createSession(transacted, sessionMode);

            return new JmsXASessionProxy(getConfiguration(), getVersionSupport(), session, manager);
        }
    }
}
