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

import java.beans.ExceptionListener;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.XAConnection;
import jakarta.jms.XAConnectionFactory;

/**
 * Base class for the the actual pooled connection factories that will be used to wrapped
 * proxy instances to the provider resources which then manage their lifetime and shared state.
 *
 * @param <CF> The type of the implementation connection proxy factory
 * @param <CP> The type of the connection proxy created by this factory
 */
public abstract class JmsPoolAbstractConnectionProxyFactory<CF, CP extends JmsPoolAbstractConnectionProxy<?, ?>> {

    private final JmsPoolConnectionConfiguration configuration = new JmsPoolConnectionConfiguration();
    private final JmsPoolAbstractConnectionPool<CP> connectionPool;

    /**
     * Creates the pooling connection factory in the started state but the application must configure
     * a backing {@link ConnectionFactory} before using any method in this object.
     */
    public JmsPoolAbstractConnectionProxyFactory() {
        this.connectionPool = createConnectionPool();
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

    /**
     * Gets the number of connections currently in the pool at the time of this call.
     *
     * @return the number of Connections currently in the Pool if started, otherwise returns zero.
     */
    public int getNumConnections() {
        return connectionPool.getConnectionCount();
    }

    //----- Pooled Connection Configuration ----------------------------------//

    /**
     * Returns the currently configured maximum idle sessions per connection which by
     * default matches the configured maximum active sessions per connection.
     *
     * @return the number if idle sessions allowed per connection before they are closed.
     *
     * @see setMaxSessionsPerConnection
     * @see setMaxIdleSessionsPerConnection
     */
    public int getMaxIdleSessionsPerConnection() {
        return configuration.getMaxIdleSessionsPerConnection();
    }

    /**
     * Sets the configured maximum idle sessions per connection which by default matches the
     * configured maximum active sessions per connection. This option allows the pool to be
     * configured to close sessions that are returned to the pool if the number of idle (not
     * in use Sessions) exceeds this amount which can reduce the amount of resources that are
     * allocated but not in use.
     * <p>
     * If the application in use opens and closes large amounts of sessions then leaving this
     * option at the default means that there is a higher chance that an idle session will be
     * available in the pool without the need to create a new instance however this does allow
     * for more idle resources to exist so in cases where turnover is low with only occasional
     * bursts in workloads it can be advantageous to lower this value to allow sessions to be
     * fully closed on return to the pool if there are already enough idle sessions to exceed
     * this amount.
     * <p>
     * If the max idle sessions per connection is configured larger than the max sessions value
     * it will be truncated to the max sessions value to conform to the total limit on how many
     * sessions can exists at any given time on a per connection basis.
     *
     * @param maxIdleSessionsPerConnection
     *    the number of idle sessions allowed per connection before they are closed.
     *
     * @see setMaxSessionsPerConnection
     */
    public void setMaxIdleSessionsPerConnection(int maxIdleSessionsPerConnection) {
        configuration.setMaxIdleSessionsPerConnection(maxIdleSessionsPerConnection);
    }

    /**
     * Returns the currently configured maximum number of sessions a pooled Connection will
     * create before it either blocks or throws an exception when a new session is requested,
     * depending on configuration.
     *
     * @return the number of session instances that can be taken from a pooled connection.
     *
     * @see setMaxSessionsPerConnection
     * @see setMaxIdleSessionsPerConnection
     */
    public int getMaxSessionsPerConnection() {
        return configuration.getMaxSessionsPerConnection();
    }

    /**
     * Sets the maximum number of pooled sessions allowed per connection.
     * <p>
     * A Connection that is created from this JMS Connection pool can limit the number
     * of Sessions that are created and loaned out.  When a limit is in place the client
     * application must be prepared to respond to failures or hangs of the various
     * {@link Connection#createSession()} methods.
     * <p>
     * Because Connections can be borrowed and returned at will the available Sessions for
     * a Connection in the pool can change dynamically so even on fresh checkout from this
     * pool a Connection may not have any available Session instances to loan out if a limit
     * is configured.
     *
     * @param maxSessionsPerConnection
     *      The maximum number of pooled sessions per connection in the pool.
     */
    public void setMaxSessionsPerConnection(int maxSessionsPerConnection) {
        configuration.setMaxSessionsPerConnection(maxSessionsPerConnection);
    }

    /**
     * Returns whether a pooled Connection will enter a blocked state or will throw an Exception
     * once the maximum number of sessions has been borrowed from the the Session Pool.
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     *
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public boolean isBlockIfSessionPoolIsFull() {
        return configuration.isBlockIfSessionPoolIsFull();
    }

    /**
     * Controls the behavior of the internal session pool. By default the call to
     * {@link Connection#createSession()} will block if the session pool is full.  If the
     * block options is set to false, it will change the default behavior and instead the
     * call to create a {@link Session} will throw a JMSException.
     * <p>
     * The size of the session pool is controlled by the {@link #getMaxSessionsPerConnection()}
     * configuration property.
     *
     * @param block
     * 		if true, the call to {@link Connection#createSession()} blocks if the session pool is full
     *      until a session is available.  defaults to true.
     */
    public void setBlockIfSessionPoolIsFull(boolean block) {
        configuration.setBlockIfSessionPoolIsFull(block);
    }

    /**
     * Gets the idle timeout value applied to Connection's that are created by this pool but are
     * not currently in use.
     *
     * @return the connection idle timeout value in (milliseconds).
     */
    public int getConnectionIdleTimeout() {
        return configuration.getConnectionIdleTimeout();
    }

    /**
     * Sets the idle timeout value for Connection's that are created by this pool but not in use in
     * Milliseconds (defaults to 30 seconds).
     * <p>
     * For a Connection that is in the pool but has no current users the idle timeout determines how
     * long the Connection can live before it is eligible for removal from the pool.  Normally the
     * connections are tested when an attempt to check one out occurs so a Connection instance can sit
     * in the pool much longer than its idle timeout if connections are used infrequently.  To evict idle
     * connections in a more timely manner the {@link #setConnectionCheckInterval(long)} can be configured
     * to a non-zero value and the pool will actively check for idle connections that have exceeded their
     * idle timeout value.
     *
     * @param connectionIdleTimeout
     *      The maximum time a pooled Connection can sit unused before it is eligible for removal.
     *
     * @see #setConnectionCheckInterval(long)
     */
    public void setConnectionIdleTimeout(int connectionIdleTimeout) {
        configuration.setConnectionIdleTimeout(connectionIdleTimeout);
    }

    /**
     * Should Sessions use one anonymous producer for all producer requests or should a new
     * MessageProducer be created for each request to create a producer object, default is true.
     * <p>
     * When enabled the session only needs to allocate one MessageProducer for all requests and
     * the MessageProducer#send(destination, message) method can be used.  Normally this is the
     * right thing to do however it does result in the Broker not showing the producers per
     * destination.
     *
     * @return true if a pooled Session will use only a single anonymous message producer instance.
     */
    public boolean isUseAnonymousProducers() {
        return configuration.isUseAnonymousProducers();
    }

    /**
     * Sets whether a pooled Session uses only one anonymous MessageProducer instance or creates
     * a new MessageProducer for each call the create a MessageProducer.
     *
     * @param anonymousProducers
     *      Boolean value that configures whether anonymous producers are used.
     */
    public void setUseAnonymousProducers(boolean anonymousProducers) {
        configuration.setUseAnonymousProducers(anonymousProducers);
    }

    /**
     * Returns the currently configured producer cache size that will be used in a pooled
     * Session when the pooled Session is not configured to use a single anonymous producer.
     *
     * @return the current explicit producer cache size.
     */
    public int getExplicitProducerCacheSize() {
        return configuration.getExplicitProducerCacheSize();
    }

    /**
     * Sets whether a pooled Session uses a cache for MessageProducer instances that are
     * created against an explicit destination instead of creating new MessageProducer on each
     * call to {@linkplain Session#createProducer(jakarta.jms.Destination)}.
     * <p>
     * When caching explicit producers the cache will hold up to the configured number of producers
     * and if more producers are created than the configured cache size the oldest or lest recently
     * used producers are evicted from the cache and will be closed when all references to that
     * producer are explicitly closed or when the pooled session instance is closed.  By default this
     * value is set to zero and no caching is done for explicit producers created by the pooled session.
     * <p>
     * This caching would only be done when the {@link #setUseAnonymousProducers(boolean)} configuration
     * option is disabled.
     *
     * @param cacheSize
     * 		The number of explicit producers to cache in the pooled Session
     */
    public void setExplicitProducerCacheSize(int cacheSize) {
        configuration.setExplicitProducerCacheSize(cacheSize);
    }

    /**
     * Returns the timeout to use for blocking creating new sessions
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     *
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return configuration.getBlockIfSessionPoolIsFullTimeout();
    }

    /**
     * Controls the behavior of the internal {@link Session} pool. By default the call to
     * Connection.getSession() will block if the {@link Session} pool is full.  This setting
     * will affect how long it blocks and throws an exception after the timeout.
     * <p>
     * The size of the session pool is controlled by the {@link #setMaxSessionsPerConnection(int)}
     * value that has been configured.  Whether or not the call to create session blocks is controlled
     * by the {@link #setBlockIfSessionPoolIsFull(boolean)} property.
     * <p>
     * By default the timeout defaults to -1 and a blocked call to create a Session will
     * wait indefinitely for a new {@link Session}
     *
     * @param blockIfSessionPoolIsFullTimeout
     * 		if blockIfSessionPoolIsFullTimeout is true then use this setting
     *      to configure how long to block before an error is thrown.
     *
     * @see #setMaxSessionsPerConnection(int)
     */
    public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
        configuration.setBlockIfSessionPoolIsFullTimeout(blockIfSessionPoolIsFullTimeout);
    }

    /**
     * Gets the configured value for the fault tolerance of pooled connections.
     *
     * @return if the pool is configured to assume connections are fault tolerant.
     */
    public boolean isFaultTolerantConnections() {
        return configuration.isFaultTolerantConnections();
    }

    /**
     * Controls if the pool will consider the provider connection as being fault tolerant.
     * <p>
     * Some JMS providers can provide a connection that will perform reconnection and error
     * handling in a generally transparent manner which would allow the pool to not close the
     * provider connection on any call to the {@link ExceptionListener} the pool assigns to all
     * pooled connections. This can reduce error handling needed in some client usage scenarios
     * since it can be assumed that the connection remains open and usable regardless of any
     * exception being thrown from JMS client APIs.
     * <p>
     * While this option is provided it can lead to broken connections being stuck in the pool
     * and be loaned out to client code with no means of closing them down and recovering. The
     * user who activates this configuration option assumes the risk here and must ensure that
     * all cases where the provider connection might still fail either due to configured reconnect
     * limits or because of fatal event that the providers server might issue are mitigated.
     *
     * @param faultTolerantConnections
     * 		Boolean value indicating if the pool should assume connection are fault tolerant.
     */
    public void setFaultTolerantConnections(boolean faultTolerantConnections) {
        configuration.setFaultTolerantConnections(faultTolerantConnections);
    }

    //----- Connection Factory Configuration -------------------------------------------//

    /**
     * Returns the maximum number to pooled Connections that this factory will allow before it
     * begins to return existing connections from the pool on calls to ({@link #createConnection}.
     *
     * @return the maxConnections that will be created for this pool.
     */
    public int getMaxConnections() {
        return connectionPool.getMaxConnections();
    }

    /**
     * Sets the maximum number of pooled Connections (defaults to one).  Each call to
     * {@link #createConnection} will result in a new Connection being created up to the max
     * connections value, once the maximum Connections have been created Connections are served
     * in a last in first out ordering.
     *
     * @param maxConnections
     * 		the maximum Connections to pool for a given user / password combination.
     */
    public void setMaxConnections(int maxConnections) {
        connectionPool.setMaxConnections(maxConnections);
    }

    /**
     * Gets the currently configured connection check interval for this connection factory.
     *
     * @return the number of milliseconds to sleep between runs of the connection check thread.
     */
    public long getConnectionCheckInterval() {
        return connectionPool.getConnectionCheckInterval();
    }

    /**
     * Sets the number of milliseconds to sleep between runs of the Connection check thread.
     * When non-positive, no connection check thread will be run, and Connections will only be
     * checked on borrow to determine if they are still valid and can continue to be used or should
     * be closed and or evicted from the pool.
     * <p>
     * By default this value is set to -1 and a connection check thread is not started.
     *
     * @param connectionCheckInterval
     *      The time to wait between runs of the Connection check thread.
     *
     * @see #setConnectionIdleTimeout(int)
     */
    public void setConnectionCheckInterval(long connectionCheckInterval) {
        connectionPool.setConnectionCheckInterval(connectionCheckInterval);
    }

    //----- Internal implementation ------------------------------------------//

    JmsPoolConnectionConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Called when a new connection pool is needed during the factory create to allow the subclass
     * to provide an appropriate instance that creates type suited to the type of JMS connection that
     * is being pooled.
     *
     * @return a new internal JMS connection pooling instance for use by this factory.
     */
    protected abstract JmsPoolAbstractConnectionPool<CP> createConnectionPool();

}
