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

/**
 * A JMS provider which pools Connection, Session and MessageProducer instances so it can be
 * used with tools like <a href="http://camel.apache.org/">Camel</a> or any other project that is
 * configured using JMS {@link ConnectionFactory} resources, connections, sessions and producers are
 * returned to a pool after use so that they can be reused later without having to undergo the cost
 * of creating them again.
 *
 * This pooling connection factory groups connections into groups based on the user name and password
 * used to create the connections along with a group for connections created without a user-name or a
 * password. The configuration for max connections applies to each group of connections individually
 * meaning to total number of connections can be greater than the configured if connections are created
 * for multiple users.
 *
 * <b>NOTE:</b> while this implementation does allow the creation of a collection of active consumers,
 * it does not 'pool' consumers. Pooling makes sense for connections, sessions and producers, which
 * are expensive to create and can remain idle a minimal cost. Consumers, on the other hand, are usually
 * just created at startup and left active, handling incoming messages as they come. When a consumer is
 * complete, it is best to close it rather than return it to a pool for later reuse: this is because,
 * even if a consumer is idle, the broker may keep delivering messages to the consumer's prefetch buffer,
 * where they'll get held until the consumer is active again.
 *
 * If you are creating a collection of consumers (for example, for multi-threaded message consumption), you
 * might want to consider using a lower prefetch value for each consumer (e.g. 10 or 20), to ensure that
 * all messages don't end up going to just one of the consumers. See this FAQ entry for more detail:
 * http://activemq.apache.org/i-do-not-receive-messages-in-my-second-consumer.html
 *
 * Optionally, one may configure the pool to examine and possibly evict objects as they sit idle in the
 * pool. This is performed by a "connection check" thread, which runs asynchronously. Caution should
 * be used when configuring this optional feature. Connection check runs contend with client threads for
 * access to resources in the pool, so if they run too frequently performance issues may result. The
 * connection check thread may be configured using the {@link #setConnectionCheckInterval(long)}
 * method. By default the value is -1 which means no connection check thread will be run. Set to a
 * non-negative value to configure the connection check thread to run, the implementation may enforce
 * a minimum time between eviction checks.
 */
public final class JmsPoolConnectionFactory extends JmsPoolAbstractConnectionFactory<JmsPoolConnectionProxyFactory> {

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
