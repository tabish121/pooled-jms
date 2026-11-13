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

import org.messaginghub.pooled.jms.JmsPoolAbstractConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactoryStatistics;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.XAConnection;
import jakarta.jms.XAConnectionFactory;

/**
 * Base class for the the actual pooled connection factories that will be used to wrapped
 * proxy instances to the provider resources which then manage their lifetime and shared state.
 *
 * @param <CF> The actual type of the Connection factory this proxy factory uses
 * @param <CP> The type of the connection proxy created by this factory
 */
public abstract class JmsAbstractConnectionProxyFactory<CF, CP extends JmsAbstractConnectionProxy<CP, ?>> {

    private final JmsConnectionProxyConfiguration configuration = new JmsConnectionProxyConfiguration();
    private final JmsAbstractConnectionPool<CP> connectionPool;

    /**
     * Creates the pooling connection factory in the started state but the application must configure
     * a backing {@link ConnectionFactory} before using any method in this object.
     */
    public JmsAbstractConnectionProxyFactory() {
        this.connectionPool = createConnectionPool();
    }

    /**
     * {@return an instance of the statistics API for this pooling connection proxy factory}
     */
    public JmsPoolConnectionFactoryStatistics getStatistics() {
        return connectionPool;
    }

    /**
     * Gets the configured {@link ConnectionFactory} or {@link XAConnectionFactory} that is used when new
     * {@link Connection} or {@link XAConnection} instance are added to the pool.
     *
     * @return the currently configured ConnectionFactory used to create the pooled Connections.
     */
    public abstract CF getConnectionFactory();

    /**
     * Sets the {@link ConnectionFactory} or {@link XAConnectionFactory} used to create new pooled Connections.
     * <p>
     * Updates to this value do not affect Connections that were previously created and placed
     * into the pool. In order to allocate new Connections based off this new ConnectionFactory
     * it is first necessary to {@link #clear} the pooled Connections.
     *
     * @param factory
     *      The factory to use to create pooled Connections (cannot be null).
     */
    public abstract void setConnectionFactory(final CF factory);

    /**
     * Returns a JMS {@link Connection} from the pool which is either a new connection if the pool has
     * capacity to add more connections or a reference to an existing connection that is already in the
     * pool and is possibly already shared with the previous caller of this method.
     *
     * @param userName
     * 		The user name to use to connect to the remote peer
     * @param password
     * 		The password to use to connect to the remote peer
     *
     * @return a wrapped JMS connection that is obtained from the connection pool
     *
     * @throws JMSException if an error occurs while creating the connection.
     */
    public CP createConnection(String userName, String password) throws JMSException {
        if (getConnectionFactory() == null) {
            throw new IllegalStateException("Cannot create a connection before a JMS ConnectionFactory has been assigned.");
        }

        return connectionPool.nextAvailable(userName, password);
    }

    //----- Setup and Close --------------------------------------------------//

    /**
     * Start the pooled connection factory if not already started.
     * <p>
     * A pooling connection factory can be stopped which closes all pooled connections and
     * prevents any new connection from being taken from the pool. Starting the factory will
     * enable taking new connections from the pool but does not allocate any new connections
     * when called.
     *
     * @throws IllegalStateException if the provider connection factory has not been configured
     */
    public void start() throws IllegalStateException {
        if (getConnectionFactory() == null) {
            throw new IllegalStateException("No JMS client ConnectionFactory instance has been configured");
        }

        connectionPool.start();
    }

    /**
     * Stops the pool from providing any new connections and closes all pooled Connections.
     * <p>
     * This method stops services from the JMS Connection Pool closing down any Connections in
     * the pool regardless of them being loaned out at the time. The pool cannot be restarted
     * after a call to stop.
     */
    public void stop() {
        connectionPool.stop();
    }

    /**
     * Checks if the {@link ConnectionFactory} has been stopped.
     *
     * @return <code>true</code> if the JMS connection pool is stopped at the time of this call.
     */
    public boolean isStopped() {
        return connectionPool.isStopped();
    }

    /**
     * Clears all connections from the pool. Each connection that is currently in the pool is
     * closed and removed from the pool. A new connection will be created on the next call to
     * a create connection variant if the pool has not been stopped. Care should be taken when
     * using this method as Connections that are in use by the client will be closed. It is also
     * possible for connections to remain in the pool after this method returns if it raced with
     * other threads calling a create connection variant.
     */
    public void clear() {
        connectionPool.clear();
    }

    //----- Pooled Connection Configuration ----------------------------------//

    /**
     * @see JmsPoolAbstractConnectionFactory#getMaxIdleSessionsPerConnection()
     */
    public int getMaxIdleSessionsPerConnection() {
        return configuration.getMaxIdleSessionsPerConnection();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setMaxIdleSessionsPerConnection(int)
     */
    public void setMaxIdleSessionsPerConnection(int maxIdleSessionsPerConnection) {
        configuration.setMaxIdleSessionsPerConnection(maxIdleSessionsPerConnection);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#getMaxSessionsPerConnection()
     */
    public int getMaxSessionsPerConnection() {
        return configuration.getMaxSessionsPerConnection();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setMaxSessionsPerConnection(int)
     */
    public void setMaxSessionsPerConnection(int maxSessionsPerConnection) {
        configuration.setMaxSessionsPerConnection(maxSessionsPerConnection);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#isBlockIfSessionPoolIsFull()
     */
    public boolean isBlockIfSessionPoolIsFull() {
        return configuration.isBlockIfSessionPoolIsFull();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setBlockIfSessionPoolIsFull(boolean)
     */
    public void setBlockIfSessionPoolIsFull(boolean block) {
        configuration.setBlockIfSessionPoolIsFull(block);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#getConnectionIdleTimeout()
     */
    public int getConnectionIdleTimeout() {
        return configuration.getConnectionIdleTimeout();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setConnectionIdleTimeout(int)
     */
    public void setConnectionIdleTimeout(int connectionIdleTimeout) {
        configuration.setConnectionIdleTimeout(connectionIdleTimeout);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#isUseAnonymousProducers()
     */
    public boolean isUseAnonymousProducers() {
        return configuration.isUseAnonymousProducers();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setUseAnonymousProducers(boolean)
     */
    public void setUseAnonymousProducers(boolean anonymousProducers) {
        configuration.setUseAnonymousProducers(anonymousProducers);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#getExplicitProducerCacheSize()
     */
    public int getExplicitProducerCacheSize() {
        return configuration.getExplicitProducerCacheSize();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setExplicitProducerCacheSize(int)
     */
    public void setExplicitProducerCacheSize(int cacheSize) {
        configuration.setExplicitProducerCacheSize(cacheSize);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#getBlockIfSessionPoolIsFullTimeout()
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return configuration.getBlockIfSessionPoolIsFullTimeout();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setBlockIfSessionPoolIsFullTimeout(long)
     */
    public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
        configuration.setBlockIfSessionPoolIsFullTimeout(blockIfSessionPoolIsFullTimeout);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#isFaultTolerantConnections()
     */
    public boolean isFaultTolerantConnections() {
        return configuration.isFaultTolerantConnections();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setFaultTolerantConnections(boolean)
     */
    public void setFaultTolerantConnections(boolean faultTolerantConnections) {
        configuration.setFaultTolerantConnections(faultTolerantConnections);
    }

    //----- Connection Factory Configuration -------------------------------------------//

    /**
     * @see JmsPoolAbstractConnectionFactory#getMaxConnections()
     */
    public int getMaxConnections() {
        return connectionPool.getMaxConnections();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setMaxConnections(int)
     */
    public void setMaxConnections(int maxConnections) {
        connectionPool.setMaxConnections(maxConnections);
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#getConnectionCheckInterval()
     */
    public long getConnectionCheckInterval() {
        return connectionPool.getConnectionCheckInterval();
    }

    /**
     * @see JmsPoolAbstractConnectionFactory#setConnectionCheckInterval(long)
     */
    public void setConnectionCheckInterval(long connectionCheckInterval) {
        connectionPool.setConnectionCheckInterval(connectionCheckInterval);
    }

    //----- Internal implementation ------------------------------------------//

    JmsConnectionProxyConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Called when a new connection pool is needed during the factory create to allow the subclass
     * to provide an appropriate instance that creates type suited to the type of JMS connection that
     * is being pooled.
     *
     * @return a new internal JMS connection pooling instance for use by this factory.
     */
    protected abstract JmsAbstractConnectionPool<CP> createConnectionPool();

}
