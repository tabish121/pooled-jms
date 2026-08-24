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

import org.messaginghub.pooled.jms.internal.JmsPoolConnectionProxyFactory;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;

public class JmsPoolConnectionFactory extends JmsPoolAbstractConnectionFactory<JmsPoolConnectionProxyFactory> {

    private final JmsPoolConnectionProxyFactory proxyFactory = new JmsPoolConnectionProxyFactory();

    /**
     * {@return a reference to the configured provider Connection factory}
     */
    public ConnectionFactory getConnectionFactory() {
        return proxyFactory.getConnectionFactory();
    }

    /**
     * Sets the assigned provider Connection factory to use by this pooled Connection factory.
     *
     * @param factory
     * 		The provider Connection factory to assign to this pooled factory.
     */
    public void setConnectionFactory(ConnectionFactory factory) {
        proxyFactory.setConnectionFactory(factory);
    }

    @Override
    protected JmsPoolConnectionProxyFactory getConnectionFactoryProxy() {
        return proxyFactory;
    }

    @Override
    protected JmsPoolConnection newJmsPoolConnection(String username, String password) throws JMSException {
        return new JmsPoolConnection(getConnectionFactoryProxy().createConnection(username, password));
    }

    @Override
    protected JmsPoolJMSContext newJmsPoolContext(String username, String password, int sessionMode) throws JMSException {
        return new JmsPoolJMSContext(newJmsPoolConnection(username, password), sessionMode);
    }

    @Override
    protected JMSContext createProviderJmsContext(String username, String password, int sessionMode) {
        final ConnectionFactory factory = proxyFactory.getConnectionFactory();

        if (factory == null) {
            throw new IllegalStateRuntimeException("No ConnectionFactory instance assigned to the pool ConnectionFactory");
        }

        if (username == null && password == null) {
            return factory.createContext(sessionMode);
        } else {
            return factory.createContext(username, password, sessionMode);
        }
    }
}
