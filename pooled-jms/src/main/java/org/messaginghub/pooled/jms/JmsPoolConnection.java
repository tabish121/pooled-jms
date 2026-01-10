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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionConsumer;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSession;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.Topic;
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
public class JmsPoolConnection implements TopicConnection, QueueConnection, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final AtomicIntegerFieldUpdater<JmsPoolConnection> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolConnection.class, "closed");

    /**
     * The wrapped connection which can become null once the wrapper is closed.
     */
    protected JmsPoolSharedConnection connection;

    /**
     * An exception listener assigned by the parent pooled connection factory.
     */
    protected volatile ExceptionListener connectionExceptionListener;

    private volatile int closed;

    private final JmsPoolConnectionCallbacks callbacks = new JmsPoolConnectionCallbacks();
    private final List<TemporaryQueue> managedTempQueues = Collections.synchronizedList(new ArrayList<TemporaryQueue>());
    private final List<TemporaryTopic> managedTempTopics = Collections.synchronizedList(new ArrayList<TemporaryTopic>());
    private final List<JmsPoolSession> managedSessions = Collections.synchronizedList(new ArrayList<JmsPoolSession>());

    /**
     * Creates a new PooledConnection instance that uses the given ConnectionPool to create
     * and manage its resources.  The ConnectionPool instance can be shared amongst many
     * PooledConnection instances.
     *
     * @param pool
     *      The connection and pool manager backing this proxy connection object.
     */
    public JmsPoolConnection(JmsPoolSharedConnection pool) {
        this.connection = pool;
        this.connection.addWrapperExceptionListener(callbacks);
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            cleanupManagedConnectionResources();
            if (connection != null) {
                connectionExceptionListener = null;
                connection.removeWrapperExceptionListener(callbacks);
                connection.decrementReferenceCount();
                connection = null;
            }
        }
    }

    @Override
    public void start() throws JMSException {
        checkClosed();
        connection.start();
    }

    @Override
    public void stop() throws JMSException {
        checkClosed();
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return getConnection().getMetaData();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosed();
        return connectionExceptionListener != null ? connectionExceptionListener : connection.getConnectionFactoryExceptionListener();
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        checkClosed();
        connectionExceptionListener = exceptionListener;
    }

    @Override
    public String getClientID() throws JMSException {
        return getConnection().getClientID();
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        // ignore repeated calls to setClientID() with the same client id
        // this could happen when a JMS component such as Spring that uses a
        // Pooled JMS ConnectionFactory when it shuts down and reinitializes.
        if (getConnection().getClientID() == null || !getClientID().equals(clientID)) {
            getConnection().setClientID(clientID);
        }
    }

    //----- ConnectionConsumer factory methods -------------------------------//

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String selector, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(destination, selector, serverSessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(topic, s, serverSessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String selector, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return getConnection().createDurableConnectionConsumer(topic, selector, s1, serverSessionPool, i);
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String selector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        connection.checkClientJMSVersionSupport(2, 0);
        return getConnection().createSharedDurableConnectionConsumer(topic, subscriptionName, selector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String selector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        checkClosed();
        connection.checkClientJMSVersionSupport(2, 0);
        return getConnection().createSharedConnectionConsumer(topic, subscriptionName, selector, sessionPool, maxMessages);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String selector, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(queue, selector, serverSessionPool, maxMessages);
    }

    //----- Session factory methods ------------------------------------------//

    @Override
    public QueueSession createQueueSession(boolean transacted, int ackMode) throws JMSException {
        return (QueueSession) createSession(transacted, ackMode);
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int ackMode) throws JMSException {
        return (TopicSession) createSession(transacted, ackMode);
    }

    @Override
    public Session createSession() throws JMSException {
        return createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        return createSession(sessionMode == Session.SESSION_TRANSACTED, sessionMode);
    }

    @Override
    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        checkClosed();

        final JmsPoolSession result = (JmsPoolSession) connection.createSession(transacted, ackMode);

        // Store the session so we can close the sessions that this Pooled JMS Connection
        // created in order to ensure that consumers etc are closed per the JMS contract.
        managedSessions.add(result);

        // Add a event listener to the session that notifies us when the session
        // creates / destroys temporary destinations and closes etc.
        result.addSessionEventListener(callbacks);

        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + connection + " }";
    }

    /**
     * Provides access to the underlying provider JMS {@link Connection}.
     * <p>
     * This is not thread safe and is not meant for general use and its results are undefined.
     *
     * @return the {@link Connection} from the JMS provider that is being pooled.
     *
     * @throws JMSException if an error occurs while trying to access the underlying connection.
     */
    public Connection getConnection() throws JMSException {
        checkClosed();
        return connection.getConnection();
    }

    //----- Metrics APIs -----------------------------------------------------//

    /**
     * Gets the total number of sessions managed by this connection wrapper.
     *
     * @return the total number of Pooled session including idle sessions that are not
     *          currently loaned out to any client.
     *
     * @throws JMSException if the connection has been closed.
     */
    public int getNumSessions() throws JMSException {
        checkClosed();
        return connection.getNumSessions();
    }

    /**
     * Gets the current number of active sessions loaned out by this wrapped connection.
     *
     * @return the number of Sessions that are currently checked out of this Connection's session pool.
     *
     * @throws JMSException if the connection has been closed.
     */
    public int getNumActiveSessions() throws JMSException {
        checkClosed();
        return connection.getNumActiveSessions();
    }

    /**
     * Gets the number of idle sessions managed by this connection wrapper.
     *
     * @return the number of Sessions that are idle in this Connection's sessions pool.
     *
     * @throws JMSException if the connection has been closed.
     */
    public int getNumtIdleSessions() throws JMSException {
        checkClosed();
        return connection.getNumIdleSessions();
    }

    //----- Internal support methods -----------------------------------------//

    /**
     * Gets the state of the connection closed flag.
     *
     * @return <code>true</code> if the connection has been closed.
     */
    protected boolean isClosed() {
        return closed > 0;
    }

    /**
     * Checks for closure of this connection wrapper and throws if true.
     *
     * @throws IllegalStateException if the connection is closed.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("Connection has already been closed");
        }
    }

    /*
     * Cleans up resources created in this wrapped connection instance which are isolated from other
     * instances which may also be sharing this same pooled connection.
     *
     * Remove all of the temporary destinations created for this connection. This is important since
     * the underlying connection may be reused over a long period of time, accumulating all of the
     * temporary destinations from each use. However, from the perspective of the life-cycle from the
     * client's view, close() closes the connection and, therefore, deletes all of the temporary
     * destinations created.
     *
     * The pooled connection tracks all Sessions that it created and now we close them. Closing the
     * pooled session will return the internal Session to the Pool of Session after cleaning up all
     * the resources that the Session had allocated for this PooledConnection.
     */
    private void cleanupManagedConnectionResources() {
        managedTempQueues.removeIf(tempQueue -> {
            try {
                tempQueue.delete();
            } catch (JMSException ex) {
                LOG.info("failed to delete Temporary Queue \"" + tempQueue + "\" on closing pooled connection: " + ex.getMessage());
            }

            return true;
        });

        managedTempTopics.removeIf(tempTopic -> {
            try {
                tempTopic.delete();
            } catch (JMSException ex) {
                LOG.info("failed to delete Temporary Topic \"" + tempTopic + "\" on closing pooled connection: " + ex.getMessage());
            }

            return true;
        });

        managedSessions.removeIf(session -> {
            try {
                session.close();
            } catch (JMSException ex) {
                LOG.info("failed to close loaned Session \"" + session + "\" on closing pooled connection: " + ex.getMessage());
            }

            return true;
        });
    }

    //----- Internal resource callback APIs ----------------------------------//

    private class JmsPoolConnectionCallbacks implements JmsPoolSessionEventListener, ExceptionListener {

        @Override
        public void onException(JMSException exception) {
            final ExceptionListener listener = connectionExceptionListener;

            if (!isClosed() && listener != null) {
                try {
                    listener.onException(exception);
                } catch (Exception ex) {
                    LOG.trace("Client configured exception listener threw exception which will be ignored", ex);
                }
            }
        }

        @Override
        public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
            if (!isClosed()) {
                managedTempQueues.add(tempQueue);
            } else {
                try {
                    tempQueue.delete(); // Race on close and create requires delete to avoid a leak
                } catch (Exception ex) {
                    LOG.debug("failed to delete Temporary Queue \"" + tempQueue + "\" on closing pooled connection: " + ex.getMessage());
                }
            }
        }

        @Override
        public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
            if (!isClosed()) {
                managedTempTopics.add(tempTopic);
            } else {
                try {
                    tempTopic.delete(); // Race on close and create requires delete to avoid a leak
                } catch (Exception ex) {
                    LOG.debug("failed to delete Temporary Topic \"" + tempTopic + "\" on closing pooled connection: " + ex.getMessage());
                }
            }
        }

        @Override
        public void onSessionClosed(JmsPoolSession session) {
            if (!isClosed()) {
                managedSessions.remove(session);
            } else {
                try {
                    session.close(); // Race on close and create requires session close to avoid a leak
                } catch (JMSException ex) {
                    LOG.debug("failed to close loaned Session \"" + session + "\" on closing pooled connection: " + ex.getMessage());
                }
            }
        }
    }
}
