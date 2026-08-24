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

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NamingEnumeration;
import javax.naming.spi.ObjectFactory;

import org.messaginghub.pooled.jms.internal.JmsPoolXAConnectionProxyFactory;
import org.messaginghub.pooled.jms.util.IntrospectionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.XAConnection;
import jakarta.jms.XAConnectionFactory;
import jakarta.jms.XAJMSContext;
import jakarta.jms.XAQueueConnection;
import jakarta.jms.XAQueueConnectionFactory;
import jakarta.jms.XATopicConnection;
import jakarta.jms.XATopicConnectionFactory;
import jakarta.transaction.TransactionManager;

public class JmsPoolXAConnectionFactory extends JmsPoolAbstractConnectionFactory<JmsPoolXAConnectionProxyFactory> implements ObjectFactory, Serializable, XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final long serialVersionUID = 7753681333583183646L;

    /**
     * The connection proxy factory that creates the wrapped XAConnection instances for this factory.
     */
    private final JmsPoolXAConnectionProxyFactory proxyFactory = new JmsPoolXAConnectionProxyFactory();

    /**
     * Transaction manager that must be assigned for enlistment to take affect
     */
    private TransactionManager transactionManager;

    /**
     * Set if the transaction manager comes from JNDI configuration
     */
    private boolean tmFromJndi;

    /**
     * The JDNI name for the configuration from this connection factory
     */
    private String tmJndiName = "java:/TransactionManager";

    /**
     * Creates the pooling connection factory in the started state but the application must configure
     * a backing {@link XAConnectionFactory} before using any method in this object.
     */
    public JmsPoolXAConnectionFactory() {
        super();
    }

    /**
     * {@return a reference to the configured provider Connection factory}
     */
    public XAConnectionFactory getConnectionFactory() {
        return proxyFactory.getConnectionFactory();
    }

    /**
     * Sets the assigned provider Connection factory to use by this pooled Connection factory.
     *
     * @param factory
     * 		The provider Connection factory to assign to this pooled factory.
     */
    public void setConnectionFactory(XAConnectionFactory factory) {
        proxyFactory.setConnectionFactory(factory);
    }

    @Override
    protected JmsPoolXAConnectionProxyFactory getConnectionFactoryProxy() {
        return proxyFactory;
    }

    @Override
    protected JmsPoolXAConnection newJmsPoolConnection(String username, String password) throws JMSException {
        return new JmsPoolXAConnection(getConnectionFactoryProxy().createConnection(username, password), transactionManager);
    }

    @Override
    protected JmsPoolXAJMSContext newJmsPoolContext(String username, String password, int sessionMode) throws JMSException {
        return new JmsPoolXAJMSContext(newJmsPoolConnection(username, password), sessionMode);
    }

    @Override
    protected XAJMSContext createProviderJmsContext(String username, String password, int sessionMode) {
        final XAConnectionFactory factory = proxyFactory.getConnectionFactory();

        if (factory == null) {
            throw new IllegalStateRuntimeException("No XAConnectionFactory instance assigned to the pool XAConnectionFactory");
        }

        if (username == null && password == null) {
            return factory.createXAContext();
        } else {
            return factory.createXAContext(username, password);
        }
    }

    //----- API to manage the XA Connection Transaction manager

    /**
     * Gets the assigned {@link TransactionManager} this connection factory will use when creating
     * new {@link XAConnection} instances.
     *
     * @return a {@link TransactionManager} either from assignment for from JNDI.
     */
    public TransactionManager getTransactionManager() {
        if (transactionManager == null && tmFromJndi) {
            try {
                transactionManager = (TransactionManager) new InitialContext().lookup(getTmJndiName());
            } catch (Throwable ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("exception on tmFromJndi: " + getTmJndiName(), ignored);
                }
            }
        }
        return transactionManager;
    }

    /**
     * Sets the {@link TransactionManager} to use when creating new JMS {@link XAConnection} instances.
     *
     * @param transactionManager
     * 	The transaction manager to use when creating new connections.
     */
    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * Gets the assigned JNDI name for this pooled {@link XAConnectionFactory} instance.
     *
     * @return the configured JNDI name for this {@link XAConnectionFactory}
     */
    public String getTmJndiName() {
        return tmJndiName;
    }

    /**
     * Sets the assigned JNDI name for this pooled {@link XAConnectionFactory} instance.
     *
     * @param tmJndiName
     * 	The assigned JNDI name for the {@link XAConnectionFactory}.
     */
    public void setTmJndiName(String tmJndiName) {
        this.tmJndiName = tmJndiName;
    }

    /**
     * Gets if this factory is resolved from JNDI
     *
     * @return <code>true</code> if configured for resolution from JNDI.
     */
    public boolean isTmFromJndi() {
        return tmFromJndi;
    }

    /**
     * Allow transaction manager resolution from JNDI (EE deployment)
     *
     * @param tmFromJndi
     * 		controls if TXN manager resolution is from JNDI
     */
    public void setTmFromJndi(boolean tmFromJndi) {
        this.tmFromJndi = tmFromJndi;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        setTmFromJndi(true);
        configFromJndiConf(obj);
        if (environment != null) {
            IntrospectionSupport.setProperties(this, (Map<String, Object>) environment);
        }
        return this;
    }

    private void configFromJndiConf(Object rootContextName) {
        if (rootContextName instanceof String) {
            String name = (String) rootContextName;
            name = name.substring(0, name.lastIndexOf('/')) + "/conf" + name.substring(name.lastIndexOf('/'));
            try {
                final InitialContext ctx = new InitialContext();
                final NamingEnumeration<Binding> bindings = ctx.listBindings(name);

                while (bindings.hasMore()) {
                    Binding bd = bindings.next();
                    IntrospectionSupport.setProperty(this, bd.getName(), bd.getObject());
                }

            } catch (Exception ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("exception on config from jndi: " + name, ignored);
                }
            }
        }
    }

    //----- XA ConnectionFactory APIs for all connection type create calls

    @Override
    public XAConnection createXAConnection() throws JMSException {
        return (XAConnection) createConnection();
    }

    @Override
    public XAConnection createXAConnection(String userName, String password) throws JMSException {
        return (XAConnection) createConnection(userName, password);
    }

    @Override
    public XAJMSContext createXAContext() {
        return (XAJMSContext) createContext();
    }

    @Override
    public XAJMSContext createXAContext(String userName, String password) {
        return (XAJMSContext) createContext(userName, password, 0);
    }

    @Override
    public XATopicConnection createXATopicConnection() throws JMSException {
        return (XATopicConnection) createConnection();
    }

    @Override
    public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException {
        return (XATopicConnection) createConnection(userName, password);
    }

    @Override
    public XAQueueConnection createXAQueueConnection() throws JMSException {
        return (XAQueueConnection) createConnection();
    }

    @Override
    public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException {
        return (XAQueueConnection) createConnection(userName, password);
    }
}
