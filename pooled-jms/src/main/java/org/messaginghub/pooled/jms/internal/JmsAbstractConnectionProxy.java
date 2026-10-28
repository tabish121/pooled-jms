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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.messaginghub.pooled.jms.JmsPoolConnectionStatistics;
import org.messaginghub.pooled.jms.util.JMSVersionSupport;
import org.messaginghub.pooled.jms.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionConsumer;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.Topic;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * <p>
 * Instances of this class are shared amongst one or more wrappers that are loaned to a JMS
 * client application that has requested to create a new connection from the factory and must
 * track the session objects that are loaned out for cleanup on close.
 *
 * @param <CP> the actual connection proxy type that this instance represents
 * @param <SP> the session proxy type that the connection proxy will be creating
 */
public abstract class JmsAbstractConnectionProxy<CP extends JmsAbstractConnectionProxy<CP, SP>,
                                                     SP extends JmsAbstractSessionProxy<SP>> implements Connection, ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Collection<ExceptionListener> exceptionListeners = new ConcurrentLinkedQueue<>();
    private final String connectionId;
    private final ReferenceCounted referenced = new ReferenceCounted();
    private final JmsConnectionProxyConfiguration configuration;
    private final JMSVersionSupport versionSupport;
    private final JmsAbstractSessionPool<SP> sessionPool;
    private final JmsConnectionProxyManager<CP> manager;

    /**
     * Shared pooled JMS Connection that all subclasses may access directly.
     */
    protected final Connection connection;

    private volatile boolean closed;
    private volatile long becameIdleAt;
    private ExceptionListener connectionFactoryExceptionListener;

    JmsAbstractConnectionProxy(JmsConnectionProxyConfiguration configuration,
                               Connection connection,
                               JmsConnectionProxyManager<CP> manager) {
        this.configuration = configuration;
        this.connection = connection;
        this.connectionId = connection.toString();
        this.versionSupport = new JMSVersionSupport(connection);
        this.sessionPool = createSessionPool(configuration);
        this.manager = manager;

        try {
            // Check if wrapped connection already had an exception listener and preserve it
            setConnectionFactoryExceptionListener(connection.getExceptionListener());

            // Replace wrapped connection exception listener to allow pooled wrapper to deal
            // with exceptions first before sending them onto any set external listener.
            connection.setExceptionListener(this);
        } catch (JMSException ex) {
            LOG.warn("Could not set exception listener on create of ConnectionPool");
        }
    }

    /**
     * {@return an instance of the statistics API for this pooling connection proxy}
     */
    public JmsPoolConnectionStatistics getStatistics() {
        return sessionPool;
    }

    protected abstract CP self();

    /**
     * Attempts to acquire an additional reference on this provider connection proxy
     *
     * @return this provider connection proxy instance
     *
     * @throws JMSException if the connection proxy is already closed
     */
    public CP acquire() throws JMSException {
        checkClosed();
        return manager.tryAcquireReference(self());
    }

    @Override
    public void close() {
        if (!isClosed()) {
            manager.tryReleaseReference(self());
        }
    }

    /**
     * Returns the proxied JMS connection instance if not already closed
     *
     * @return the proxied JMS connection instance if not already closed
     *
     * @throws JMSException if the connection is already closed
     */
    public Connection getProviderConnection() throws JMSException {
        checkClosed();
        return connection;
    }

    @Override
    public void start() throws JMSException {
        checkClosed();

        try {
            connection.start();
        } catch (Throwable error) {
            manager.invalidateConnection(self());
            throw error;
        }
    }

    @Override
    public void stop() throws JMSException {
        checkClosed();
    }

    @Override
    public String getClientID() throws JMSException {
        checkClosed();
        return connection.getClientID();
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        checkClosed();

        // ignore repeated calls to setClientID() with the same client id
        // this could happen when a JMS component such as Spring that uses a
        // Pooled JMS ConnectionFactory when it shuts down and reinitializes.
        final String currentClientId = connection.getClientID();

        if (currentClientId == null || !currentClientId.equals(clientID)) {
            connection.setClientID(clientID);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosed();
        return connection.getMetaData();
    }

    @Override
    public final SP createSession() throws JMSException {
        return doCreateSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public final SP createSession(int sessionMode) throws JMSException {
        return doCreateSession(false, sessionMode);
    }

    @Override
    public SP createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return doCreateSession(transacted, acknowledgeMode);
    }

    /**
     * Performs the session create and can be overridden by subclasses to create a more custom
     * wrapper type if needed.
     *
     * @param transacted
     * 		If the created session should be transactional
     * @param acknowledgeMode
     * 		What acknowledgement mode to use for the new session
     *
     * @return a newly obtained session from the underlying session pool
     *
     * @throws JMSException if an error occurs while creating the session wrapper
     */
    protected SP doCreateSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosed();

        final SP session;

        try {
            session = sessionPool.nextSession(transacted, acknowledgeMode);
        } catch (Exception e) {
            IllegalStateException illegalStateException = new IllegalStateException(e.toString());
            illegalStateException.initCause(e);
            throw illegalStateException;
        }

        return session;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        return connection.createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        return connection.createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return connection.createSharedConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        versionSupport.enforceSharedSubscriptionSupport();
        return connection.createSharedDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    //----- API dealing with ExceptionListener registration and management

    /**
     * Gets the currently assigned {@link ExceptionListener} that was assigned from the connection factory.
     *
     * @return the ExceptionListener that was assigned to the connection factory at create of this connection
     */
    public ExceptionListener getConnectionFactoryExceptionListener() {
        return connectionFactoryExceptionListener;
    }

    /**
     * The {@link ExceptionListener} that was assigned to the pooled {@link ConnectionFactory} at the
     * time this {@link Connection} was created. This listener will be called for any exception that the
     * client library signals regardless of any loaned connection wrappers having their own exception
     * listeners registered.
     *
     * @param parentExceptionListener
     * 	The {@link ExceptionListener} that will be called for any exception from the client connection.
     */
    public void setConnectionFactoryExceptionListener(ExceptionListener parentExceptionListener) {
        this.connectionFactoryExceptionListener = parentExceptionListener;
    }

    /**
     * This is the entry point for all exceptions sent for the proxied provider connection. From here
     * we dispatch the exception to all shared connection instances that current hold a reference to this
     * connection proxy.
     */
    @Override
    public void onException(JMSException exception) {
        LOG.debug("Pooled connection onException: {}", exception.getMessage());
        LOG.trace("Pooled connection: Client exception detail", exception);

        // Closes the underlying connection and removes it from the pool if not configured
        // to assume the connection is fault tolerant and can recover on its own.
        if (!configuration.isFaultTolerantConnections()) {
            manager.invalidateConnection(self());
        }

        // Each JMS connection that comes from the pool wraps a connection holder and can
        // have its own assigned exception listener which we will call first before calling
        // any root exception listener that was configured from the parent connection factory.
        exceptionListeners.forEach(listener -> {
            try {
                listener.onException(exception);
            } catch (Exception ex) {
                LOG.trace("Ignored exception from pooled connection wrapper assigned listener:", ex);
            }
        });

        // If the provider has an exception listener from the connection factory we
        // will always call it to allow for the base level error handling to be run
        // regardless of any assigned exception that was set on the wrapper object
        // that was given to the borrowing client code.
        if (connectionFactoryExceptionListener != null) {
            try {
                connectionFactoryExceptionListener.onException(exception);
            } catch (Exception ex) {
                LOG.trace("Ignored exception from the connection factory assigned exception listener:", ex);
            }
        }
    }

    /**
     * The JMS Pool connection instances can register here to each receive a copy of any exception
     * surfaced from the proxied provider connection.
     *
     * @param consumer
     * 		The JMS {@link ExceptionListener} that will get a copy of any exception event
     *
     * @return this connection proxy instance.
     */
    public CP addExceptionConsumer(ExceptionListener consumer) {
        exceptionListeners.add(Objects.requireNonNull(consumer));
        return self();
    }

    /**
     * The JMS Pool connection instances can unregister here when it is closed so that no future
     * exception events are forwarded to it after it is closed as the proxied connection can be
     * shared amongst many pooled connection instances.
     *
     * @param consumer
     * 		The JMS {@link ExceptionListener} that should be unregistered
     *
     * @return this connection proxy instance.
     */
    public CP removeExceptionConsumer(ExceptionListener consumer) {
        exceptionListeners.remove(consumer);
        return self();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        return connectionFactoryExceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        this.connectionFactoryExceptionListener = listener;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ " + connectionId + " ]";
    }

    //----- Internal connection proxy APIs not meant for public use

    JmsConnectionProxyConfiguration getConfiguration() {
        return configuration;
    }

    JMSVersionSupport getVersionSupport() {
        return versionSupport;
    }

    Connection getConnection() {
        return connection;
    }

    void acquireReference() {
        becameIdleAt = 0;
        referenced.acquire();
    }

    boolean releaseReference() {
        final boolean unreferenced = referenced.release();

        if (unreferenced) {
            sessionPool.idle();
            becameIdleAt = System.currentTimeMillis();
        }

        return unreferenced;
    }

    void internalClose() {
        closed = true;

        try {
            sessionPool.destroy();
        } catch (Exception ex) {
            LOG.debug("Suppressed error from on session pool destroy in connection proxy.", ex);
        } finally {
            try {
                connection.close();
            } catch (Exception ex) {
                LOG.trace("Suppressed error from provider connection close in connection proxy.", ex);
            }
        }
    }

    boolean checkIsUsable() {
        final int idleTimeout = configuration.getConnectionIdleTimeout();

        boolean usable = true;

        if (isClosed()) {
            usable = false;
        } else if (referenced.isUnreferenced() && idleTimeout > 0 && becameIdleAt != 0 && ((System.currentTimeMillis() - becameIdleAt) >= idleTimeout)) {
            LOG.trace("Connection has expired or was closed: {} and will be discarded", connection);
            usable = false;
        } else {
            // Sanity check the Connection and if it throws IllegalStateException we assume
            // that it is closed or has failed due to some IO error.
            try {
                connection.getExceptionListener();
            } catch (IllegalStateException jmsISE) {
                usable = false;
            } catch (Exception ambiguous) {
                // Unsure if connection is still valid so continue as if it still is.
            }
        }

        return usable;
    }

    boolean isIdle() {
        return !referenced.isReferenced();
    }

    /**
     * The concrete connection proxy type creates the instance of a session pool and returns it
     * for use as a generic type.
     *
     * @param configuration
     * 		The immutable configuration that is applied to the created session pool
     *
     * @return the newly created session pool used by this connection proxy instance.
     */
    protected abstract JmsAbstractSessionPool<SP> createSessionPool(JmsConnectionProxyConfiguration configuration);

    /**
     * {@return <code>true</code> when the underlying provider is actually closed}
     */
    protected boolean isClosed() {
        return closed;
    }

    /**
     * Checks for the permanent shutdown of this connection proxy and throws if true.
     *
     * @throws IllegalStateException if the connection is closed permanently.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("Shared pooled Connection has already been closed");
        }
    }
}
