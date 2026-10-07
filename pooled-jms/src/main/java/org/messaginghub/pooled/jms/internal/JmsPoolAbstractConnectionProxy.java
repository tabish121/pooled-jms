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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

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
 */
public abstract class JmsPoolAbstractConnectionProxy<CP extends JmsPoolAbstractConnectionProxy<CP, SP>,
                                                     SP extends JmsPoolAbstractSessionProxy<SP>> implements Connection, ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<JmsPoolAbstractConnectionProxy> DESTROYED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolAbstractConnectionProxy.class, "destroyed");

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Collection<ExceptionListener> exceptionListeners = new ConcurrentLinkedQueue<>();
    private final String connectionId;
    private final ReferenceCounted referenced = new ReferenceCounted();
    private final JmsPoolConnectionConfiguration configuration;
    private final JMSVersionSupport versionSupport;
    private final JmsPoolAbstractSessionPool<SP> sessionPool;
    private final Consumer<CP> onConnectionClosed;
    private final Consumer<CP> onConnectionDestroyed;

    /**
     * Shared pooled JMS Connection that all subclasses may access directly.
     */
    protected final Connection connection;

    private volatile int destroyed;
    private long becameIdleAt;
    private ExceptionListener connectionFactoryExceptionListener;

    JmsPoolAbstractConnectionProxy(JmsPoolConnectionConfiguration configuration,
                                   Connection connection,
                                   Consumer<CP> onConnectionClosed,
                                   Consumer<CP> onConnectionDestroyed) {
        this.configuration = configuration;
        this.connection = connection;
        this.connectionId = connection.toString();
        this.versionSupport = new JMSVersionSupport(connection);
        this.sessionPool = createSessionPool(configuration);
        this.onConnectionClosed = onConnectionClosed;
        this.onConnectionDestroyed = onConnectionDestroyed;

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

    protected abstract CP self();

    public synchronized CP acquire() throws IllegalStateException {
        checkDestroyed();
        becameIdleAt = 0;
        referenced.acquire();
        return self();
    }

    synchronized boolean checkIsUsable() {
        final int idleTimeout = configuration.getConnectionIdleTimeout();

        boolean usable = true;

        if (isDestroyed()) {
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

    synchronized void destroy() {
        // Destroy is unrecoverable, once destroyed the underlying connection is closed.
        if (DESTROYED_UPDATER.compareAndSet(this, 0, 1)) {
            try {
                sessionPool.destroy();
            } catch (Exception ex) {
                LOG.debug("Suppressed error from on sesion pool destroy in connection proy.", ex);
            } finally {
                try {
                    connection.close();
                } catch (Exception ex) {
                    LOG.trace("Suppressed error from provider connection close in connection proy.", ex);
                } finally {
                    onConnectionDestroyed.accept(self());
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        // Closing this doesn't really close the connection or mark it as closed, it just
        // puts the connection into an idle state where it might close if the provider
        // connection closes or if an idle timeout occurs to that causes it to be destroyed.
        if (!isDestroyed() && referenced.release()) {
            sessionPool.idle();
            becameIdleAt = System.currentTimeMillis();
            onConnectionClosed.accept(self());
        }
    }

    private boolean isDestroyed() {
        return destroyed > 0;
    }

    boolean isIdle() {
        return !isDestroyed() && !referenced.isReferenced();
    }

    public Connection getProviderConnection() throws JMSException {
        checkDestroyed();
        return connection;
    }

    @Override
    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            try {
                connection.start();
            } catch (Throwable error) {
                started.set(false);
                close();
                throw error;
            }
        }
    }

    @Override
    public void stop() throws JMSException {
        if (started.compareAndSet(true, false)) {
            try {
                connection.stop();
            } catch (Throwable error) {
                started.set(false);
                destroy();
                throw error;
            }
        }
    }

    @Override
    public String getClientID() throws JMSException {
        checkDestroyed();
        return connection.getClientID();
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        checkDestroyed();

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
        checkDestroyed();
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

    protected SP doCreateSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkDestroyed();

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
        checkDestroyed();
        return connection.createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkDestroyed();
        return connection.createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkDestroyed();
        versionSupport.enforceSharedSubscriptionSupport();
        return connection.createSharedConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkDestroyed();
        versionSupport.enforceSharedSubscriptionSupport();
        return connection.createSharedDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages);
    }

    //----- Statistics APIs related to resources from this connection

    /**
     * {@return the total number of sessions both idle and actively loaned out from this connection}
     */
    public int getNumSessions() {
        return sessionPool.getSessionCount();
    }

    /**
     * {@return the total number of Sessions that are in the Session pool but not loaned out}
     */
    public int getNumIdleSessions() {
        return sessionPool.getIdleSessionCount();
    }

    /**
     * {@return the total number of Session's that have been loaned to PooledConnection instances}
     */
    public int getNumActiveSessions() {
        return sessionPool.getActiveSessionCount();
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

    @Override
    public void onException(JMSException exception) {
        LOG.debug("Pooled connection onException: {}", exception.getMessage());
        LOG.trace("Pooled connection: Client exception detail", exception);

        // Closes the underlying connection and removes it from the pool if not configured
        // to assume the connection is fault tolerant and can recover on its own.
        if (!configuration.isFaultTolerantConnections()) {
            destroy();
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
            connectionFactoryExceptionListener.onException(exception);
        }
    }

    public CP addExceptionConsumer(ExceptionListener consumer) {
        exceptionListeners.add(Objects.requireNonNull(consumer));
        return self();
    }

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

    //----- Internal helper APIs and state checks

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ " + connectionId + " ]";
    }

    JmsPoolConnectionConfiguration getConfiguration() {
        return configuration;
    }

    JMSVersionSupport getVersionSupport() {
        return versionSupport;
    }

    Connection getConnection() {
        return connection;
    }

    protected abstract JmsPoolAbstractSessionPool<SP> createSessionPool(JmsPoolConnectionConfiguration configuration);

    /**
     * Checks for the permanent shutdown of this connection proxy and throws if true.
     *
     * @throws IllegalStateException if the connection is closed permanently.
     */
    protected void checkDestroyed() throws IllegalStateException {
        if (isDestroyed()) {
            throw new IllegalStateException("Shared pooled Connection has already been closed");
        }
    }
}
