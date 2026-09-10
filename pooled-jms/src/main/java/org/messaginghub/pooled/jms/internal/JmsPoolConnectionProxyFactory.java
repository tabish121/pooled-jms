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
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;

public final class JmsPoolConnectionProxyFactory extends JmsPoolAbstractConnectionProxyFactory<ConnectionFactory, JmsPoolConnectionProxy> {

    /**
     * The assigned {@link ConnectionFactory} fur use by this proxy factory
     */
    private ConnectionFactory connectionFactory;

    /**
     * Creates the pooling connection factory in the started state but the application must configure
     * a backing {@link ConnectionFactory} before using any method in this object.
     */
    public JmsPoolConnectionProxyFactory() {
        super();
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public void setConnectionFactory(ConnectionFactory factory) {
        this.connectionFactory = factory;
    }

    @Override
    protected JmsPoolConnectionProxy createConnectionProxy(JmsPoolConnectionConfiguration configuration, Connection connection) {
        return new JmsPoolConnectionProxy(configuration, connection);
    }

    @Override
    protected Connection createProviderConnection(String userName, String password) throws JMSException {
        return getConnectionFactory().createConnection(userName, password);
    }
}
