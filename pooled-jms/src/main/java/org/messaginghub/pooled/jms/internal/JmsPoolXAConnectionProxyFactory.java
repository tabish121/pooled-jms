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

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.XAConnection;
import jakarta.jms.XAConnectionFactory;

/**
 * A pooled {@link XAConnectionFactory} that automatically enlists sessions in the
 * current active XA transaction if any.
 */
public class JmsPoolXAConnectionProxyFactory extends JmsPoolAbstractConnectionProxyFactory<XAConnectionFactory, JmsPoolXAConnectionProxy> {

    /**
     * The assigned {@link XAConnectionFactory} fur use by this proxy factory
     */
    private XAConnectionFactory connectionFactory;

    @Override
    public XAConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public void setConnectionFactory(XAConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    protected JmsPoolXAConnectionProxy createConnectionProxy(JmsPoolConnectionConfiguration configuration, Connection connection) {
        return new JmsPoolXAConnectionProxy(configuration, connection);
    }

    @Override
    protected XAConnection createProviderConnection(String userName, String password) throws JMSException {
        return getConnectionFactory().createXAConnection(userName, password);
    }
}
