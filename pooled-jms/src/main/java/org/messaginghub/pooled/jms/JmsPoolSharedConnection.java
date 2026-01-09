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

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.messaginghub.pooled.jms.util.ReferenceCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * <p>
 * Instances of this class are shared amongst one or more PooledConnection object and must
 * track the session objects that are loaned out for cleanup on close as well as ensuring
 * that the temporary destinations of the managed Connection are purged when all references
 * to this ConnectionPool are released.
 */
class JmsPoolSharedConnection implements ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final GenericKeyedObjectPool<JmsPoolSessionKey, JmsPoolSharedSession> sessionPool;
    private final Map<JmsPoolSession, JmsPoolSession> loanedSessions = new ConcurrentHashMap<>();
    private final Queue<ExceptionListener> exceptionListeners = new ConcurrentLinkedQueue<>();
    private final String connectionId;
    private final ReferenceCount referenceCount = new ReferenceCount();
    private final JmsPoolConnectionConfiguration configuration;

    protected Connection connection;

    private long lastUsed = System.currentTimeMillis();
    private boolean hasExpired;
    private int jmsMajorVersion = 1;
    private int jmsMinorVersion = 1;
    private ExceptionListener connectionFactoryExceptionListener;

    public JmsPoolSharedConnection(JmsPoolConnectionConfiguration configuration, Connection connection) {
        this.configuration = configuration;
        this.connection = connection;
        this.connectionId = connection.toString();

        try {
            // Check if wrapped connection already had an exception listener and preserve it
            setConnectionFactoryExceptionListener(connection.getExceptionListener());

            // Replace wrapped connection exception listener to allow pooled wrapper to deal
            // with exceptions first before sending them onto any set external listener.
            connection.setExceptionListener(this);
        } catch (JMSException ex) {
            LOG.warn("Could not set exception listener on create of ConnectionPool");
        }

        // Attempt to determine JMS the version support of JMS provider.
        try {
            final ConnectionMetaData connectionMetaData = connection.getMetaData();

            jmsMajorVersion = connectionMetaData.getJMSMajorVersion();
            jmsMinorVersion = connectionMetaData.getJMSMajorVersion();
        } catch (JMSException ex) {
            LOG.debug("Error while fetching JMS API version from provider, defaulting to v3.0");
            jmsMajorVersion = 3;
            jmsMinorVersion = 0;
        }

        final GenericKeyedObjectPoolConfig<JmsPoolSharedSession> poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setJmxEnabled(false);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setMaxTotalPerKey(configuration.getMaxSessionsPerConnection());
        poolConfig.setMaxIdlePerKey(
            Math.min(configuration.getMaxIdleSessionsPerConnection(), configuration.getMaxSessionsPerConnection()));
        poolConfig.setBlockWhenExhausted(configuration.isBlockIfSessionPoolIsFull());

        if (configuration.isBlockIfSessionPoolIsFull() && configuration.getBlockIfSessionPoolIsFullTimeout() > 0) {
            poolConfig.setMaxWait(Duration.ofMillis(configuration.getBlockIfSessionPoolIsFullTimeout()));
        }

        // Create our internal Pool of session instances.
        sessionPool = new GenericKeyedObjectPool<JmsPoolSessionKey, JmsPoolSharedSession>(
            new KeyedPooledObjectFactory<JmsPoolSessionKey, JmsPoolSharedSession>() {

                @Override
                public PooledObject<JmsPoolSharedSession> makeObject(JmsPoolSessionKey sessionKey) throws Exception {
                    return new DefaultPooledObject<JmsPoolSharedSession>(
                        new JmsPoolSharedSession(JmsPoolSharedConnection.this, configuration, makeSession(sessionKey)));
                }

                @Override
                public void destroyObject(JmsPoolSessionKey sessionKey, PooledObject<JmsPoolSharedSession> pooledObject) throws Exception {
                    pooledObject.getObject().close();
                }

                @Override
                public boolean validateObject(JmsPoolSessionKey sessionKey, PooledObject<JmsPoolSharedSession> pooledObject) {
                    JmsPoolSharedSession sessionHolder = pooledObject.getObject();

                    try {
                        sessionHolder.getSession().getTransacted();
                    } catch (IllegalStateException jmsISE) {
                        return false;
                    } catch (Exception ambiguous) {
                        // Unsure if session is still valid so continue as if it still is.
                    }

                    return true;
                }

                @Override
                public void activateObject(JmsPoolSessionKey sessionKey, PooledObject<JmsPoolSharedSession> pooledObject) throws Exception {
                }

                @Override
                public void passivateObject(JmsPoolSessionKey sessionKey, PooledObject<JmsPoolSharedSession> pooledObject) throws Exception {
                }

            }, poolConfig
        );
    }

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

    public synchronized Connection getConnection() {
        return connection;
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        JmsPoolSessionKey key = new JmsPoolSessionKey(transacted, ackMode);
        JmsPoolSession session;
        try {
            session = new JmsPoolSession(key, sessionPool.borrowObject(key), sessionPool, key.isTransacted());
            session.addSessionEventListener(new JmsPoolSessionEventListener() {

                @Override
                public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
                }

                @Override
                public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
                }

                @Override
                public void onSessionClosed(JmsPoolSession session) {
                    loanedSessions.remove(session);
                }
            });

            loanedSessions.put(session, session);
        } catch (Exception e) {
            IllegalStateException illegalStateException = new IllegalStateException(e.toString());
            illegalStateException.initCause(e);
            throw illegalStateException;
        }
        return session;
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                sessionPool.close();
            } catch (Exception e) {
            } finally {
                try {
                    connection.close();
                } catch (Exception e) {
                } finally {
                    connection = null;
                }
            }
        }
    }

    public boolean isClosed() {
        return connection == null;
    }

    /**
     * @return the total number of Pooled session including idle sessions that are not
     *          currently loaned out to any client.
     */
    public int getNumSessions() {
        return sessionPool.getNumIdle() + sessionPool.getNumActive();
    }

    /**
     * @return the total number of Sessions that are in the Session pool but not loaned out.
     */
    public int getNumIdleSessions() {
        return sessionPool.getNumIdle();
    }

    /**
     * @return the total number of Session's that have been loaned to PooledConnection instances.
     */
    public int getNumActiveSessions() {
        return sessionPool.getNumActive();
    }

    /**
     * Checks for JMS version support in the underlying JMS Connection this pooled connection
     * wrapper encapsulates.
     *
     * @param requiredMajor
     * 		The JMS Major version required for a feature to be supported.
     * @param requiredMinor
     * 		The JMS Minor version required for a feature to be supported.
     *
     * @return true if the Connection supports the version range given.
     */
    public boolean isJMSVersionSupported(int requiredMajor, int requiredMinor) {
        return jmsMajorVersion >= requiredMajor && jmsMinorVersion >= requiredMinor;
    }

    /**
     * {@return the ExceptionListener that was assigned to the connection factory at create of this connection}
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
            close();
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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ " + connectionId + " ]";
    }

    protected Session makeSession(JmsPoolSessionKey key) throws JMSException {
        return connection.createSession(key.isTransacted(), key.getAckMode());
    }

    synchronized void addWrapperExceptionListener(ExceptionListener listener) {
        exceptionListeners.add(Objects.requireNonNull(listener));
    }

    synchronized void removeWrapperExceptionListener(ExceptionListener listener) {
        exceptionListeners.remove(listener);
    }

    synchronized void incrementReferenceCount() {
        if (connection != null) {
            referenceCount.incrementAndGet();
            lastUsed = System.currentTimeMillis();
        }
    }

    synchronized void decrementReferenceCount() {
        if (connection != null) {
            lastUsed = System.currentTimeMillis();

            if (referenceCount.decrementAndGet() == 0) {
                // Loaned sessions are those that are active in the sessionPool and have
                // not been closed by the client before closing the connection. These need
                // to be closed so that all session's reflect the fact that the parent
                // Connection is closed.
                loanedSessions.keySet().forEach(session -> {
                    try {
                        session.close();
                    } catch (Exception e) {
                        LOG.trace("Swallowed exception when closing a loaned session: {}", session);
                    }
                });

                loanedSessions.clear();
                idleTimeoutCheck();
            }
        }
    }

    /**
     * Determines if this Connection has expired.
     * <p>
     * A PooledConnection is considered expired when all references to it are released AND the
     * configured idleTimeout has elapsed.  Once a PooledConnection is determined to have expired
     * its underlying Connection is closed.
     *
     * @return true if this connection has expired and can be closed.
     */
    synchronized boolean idleTimeoutCheck() {
        if (connection == null || hasExpired) {
            return true;
        }

        final int idleTimeout = configuration.getConnectionIdleTimeout();

        // Only set hasExpired here if no references, as a Connection with references is by
        // definition not idle at this time.
        if (referenceCount.isUnreferenced() && idleTimeout > 0 && (lastUsed + idleTimeout) - System.currentTimeMillis() < 0) {
            hasExpired = true;
            close();
        }

        return hasExpired;
    }

    void checkClientJMSVersionSupport(int requiredMajor, int requiredMinor) throws JMSException {
        checkClientJMSVersionSupport(requiredMajor, requiredMinor, false);
    }

    void checkClientJMSVersionSupport(int requiredMajor, int requiredMinor, boolean runtimeEx) throws JMSException {
        if (jmsMajorVersion >= requiredMajor && jmsMinorVersion >= requiredMinor) {
            return;
        }

        String message = "JMS v" + requiredMajor + "." + requiredMinor + " client feature requested, " +
                         "configured client supports JMS v" + jmsMajorVersion + "." + jmsMinorVersion;

        if (runtimeEx) {
            throw new JMSRuntimeException(message);
        } else {
            throw new JMSException(message);
        }
    }
}
