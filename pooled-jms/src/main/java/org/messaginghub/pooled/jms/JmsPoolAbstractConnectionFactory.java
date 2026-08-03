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

import java.beans.ExceptionListener;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.messaginghub.pooled.jms.internal.JmsPoolAbstractConnectionProxyFactory;
import org.messaginghub.pooled.jms.internal.JmsPoolConnectionProxy;
import org.messaginghub.pooled.jms.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.Session;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;

/**
 * A JMS provider which pools Connection, Session and MessageProducer instances
 * so it can be used with tools like <a href="http://camel.apache.org/">Camel</a> or any other project
 * that is configured using JMS ConnectionFactory resources, connections, sessions and producers are
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
 * connection check thread may be configured using the {@link JmsPoolAbstractConnectionFactory#setConnectionCheckInterval(long)}
 * method. By default the value is -1 which means no connection check thread will be run. Set to a
 * non-negative value to configure the connection check thread to run, the implementation may enforce
 * a minimum time between eviction checks.
 */
public abstract class JmsPoolAbstractConnectionFactory<E extends JmsPoolAbstractConnectionProxyFactory<?, ?>> implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<JmsPoolAbstractConnectionFactory> STOPPED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolAbstractConnectionFactory.class, "stopped");

    /**
     * The default value controlling time between checks for idle connections in the pool.
     */
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS = -1;

    /**
     * The default maximum number of connections to maintain in the connection pool.
     */
    public static final int DEFAULT_MAX_CONNECTIONS = 1;

    /**
     * The default value controlling if the connection pool uses its own JMS context instances or
     * provides one directly from the configuration {@link ConnectionFactory} instance.
     */
    public static final boolean DEFAULT_USE_PROVIDER_JMS_CONTEXT = false;

    private boolean useProviderJMSContext = DEFAULT_USE_PROVIDER_JMS_CONTEXT;
    private volatile int stopped;

    /**
     * Creates the pooling connection factory in the started state but the application must configure
     * a backing {@link ConnectionFactory} before using any method in this object.
     */
    public JmsPoolAbstractConnectionFactory() {}

    /**
     * Gets the configured {@link ConnectionFactory} that is used when new {@link Connection} instance are added to the pool.
     *
     * @return the currently configured ConnectionFactory used to create the pooled Connections.
     */
    protected abstract E getConnectionFactoryProxy();

    //----- JMS Connection Creation ---------------------------------------------//

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return (TopicConnection) createConnection();
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return (TopicConnection) createConnection(userName, password);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return newJmsPoolConnection(userName, password);
    }

    //----- JMS Context Creation ---------------------------------------------//

    @Override
    public JMSContext createContext() {
        return createContext(null, null, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return createContext(null, null, sessionMode);
    }

    @Override
    public JMSContext createContext(String username, String password) {
        return createContext(username, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String username, String password, int sessionMode) {
        if (isStopped()) {
            LOG.debug("The pooled connection factory is stopped, skipping create new connection.");
            throw new IllegalStateRuntimeException("Cannot create a JMS context from a stopped pooled connection factory");
        }

        if (isUseProviderJMSContext()) {
            return createProviderJmsContext(username, password, sessionMode);
        } else {
            try {
                return newJmsPoolContext(username, password, sessionMode);
            } catch (JMSException e) {
                throw JMSExceptionSupport.createRuntimeException(e);
            }
        }
    }

    //----- Setup and Close --------------------------------------------------//

    /**
     * Start the pooled connection factory if not already started.
     * <p>
     * A pooling connection factory can be stopped which closes all pooled connections and
     * prevents any new connection from being taken from the pool. Starting the factory will
     * enable taking new connections from the pool but does not allocate any new connections
     * when called.
     */
    public synchronized void start() {
        if (STOPPED_UPDATER.weakCompareAndSet(this, 1, 0)) {
            LOG.debug("JMS pooled connection factory start method called, no action performed.");
            getConnectionFactoryProxy().start();
        }
    }

    /**
     * Stops the pool from providing any new connections and closes all pooled Connections.
     * <p>
     * This method stops services from the JMS Connection Pool closing down any Connections in
     * the pool regardless of them being loaned out at the time. The pool cannot be restarted
     * after a call to stop.
     */
    public synchronized void stop() {
        if (STOPPED_UPDATER.weakCompareAndSet(this, 0, 1)) {
            final JmsPoolAbstractConnectionProxyFactory<?, ?> connectionProxyFactory = getConnectionFactoryProxy();

            LOG.debug("Stopping the pooled connection factory, number of connections in pool = {}",
                      connectionProxyFactory != null ? connectionProxyFactory.getNumConnections() : 0);
            try {
                connectionProxyFactory.stop();
            } catch (Exception ignored) {
                LOG.trace("Caught exception on close of the Connection pool during stop: ", ignored);
            }
        }
    }

    /**
     * Checks if the {@link ConnectionFactory} has been stopped.
     *
     * @return <code>true</code> if the JMS connection pool is stopped at the time of this call.
     */
    public boolean isStopped() {
        return stopped != 0;
    }

    /**
     * Clears all connections from the pool. Each connection that is currently in the pool is
     * closed and removed from the pool. A new connection will be created on the next call to
     * {@link #createConnection} if the pool has not been stopped. Care should be taken when
     * using this method as Connections that are in use by the client will be closed. It is also
     * possible for connections to remain in the pool after this method returns if it raced with
     * other threads calling a {@link #createConnection()} variant.
     */
    public synchronized void clear() {
        if (!isStopped()) {
            getConnectionFactoryProxy().clear();
        }
    }

    /**
     * Gets the number of connections currently in the pool at the time of this call.
     *
     * @return the number of Connections currently in the Pool if started, otherwise returns zero.
     */
    public int getNumConnections() {
        if (isStopped()) {
            return 0;
        } else {
            return getConnectionFactoryProxy().getNumConnections();
        }
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
        return getConnectionFactoryProxy().getMaxIdleSessionsPerConnection();
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
        getConnectionFactoryProxy().setMaxIdleSessionsPerConnection(maxIdleSessionsPerConnection);
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
        return getConnectionFactoryProxy().getMaxSessionsPerConnection();
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
        getConnectionFactoryProxy().setMaxSessionsPerConnection(maxSessionsPerConnection);
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
        return getConnectionFactoryProxy().isBlockIfSessionPoolIsFull();
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
        getConnectionFactoryProxy().setBlockIfSessionPoolIsFull(block);
    }

    /**
     * Gets the idle timeout value applied to Connection's that are created by this pool but are
     * not currently in use.
     *
     * @return the connection idle timeout value in (milliseconds).
     */
    public int getConnectionIdleTimeout() {
        return getConnectionFactoryProxy().getConnectionIdleTimeout();
    }

    /**
     * Sets the idle timeout value for Connection's that are created by this pool but not in use in
     * Milliseconds (defaults to 30 seconds).
     * <p>
     * For a Connection that is in the pool but has no current users the idle timeout determines how
     * long the Connection can live before it is eligible for removal from the pool. Normally the
     * connections are tested when an attempt to check one out occurs so a Connection instance can sit
     * in the pool much longer than its idle timeout if connections are used infrequently. To evict idle
     * connections in a more timely manner the {@link #setConnectionCheckInterval(long)} can be configured
     * to a non-zero value and the pool will actively check for idle connections that have exceeded their
     * idle timeout value.
     * <p>
     * A value of -1 disables idle connection checks while a non-zero value enables a periodic check for
     * idle connections. The task of checking connections for idle state eviction can contend with the
     * calls to create new connections if the pool size is large so care should be taken to not set the
     * check interval too low, the implementation itself reserves the right to enforce a minimum value
     * for the time between eviction runs when they are enabled.
     *
     * @param connectionIdleTimeout
     *      The maximum time a pooled Connection can sit unused before it is eligible for removal.
     *
     * @see #setConnectionCheckInterval(long)
     */
    public void setConnectionIdleTimeout(int connectionIdleTimeout) {
        getConnectionFactoryProxy().setConnectionIdleTimeout(connectionIdleTimeout);
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
        return getConnectionFactoryProxy().isUseAnonymousProducers();
    }

    /**
     * Sets whether a pooled Session uses only one anonymous MessageProducer instance or creates
     * a new MessageProducer for each call the create a MessageProducer.
     *
     * @param anonymousProducers
     *      Boolean value that configures whether anonymous producers are used.
     */
    public void setUseAnonymousProducers(boolean anonymousProducers) {
        getConnectionFactoryProxy().setUseAnonymousProducers(anonymousProducers);
    }

    /**
     * Returns the currently configured producer cache size that will be used in a pooled
     * Session when the pooled Session is not configured to use a single anonymous producer.
     *
     * @return the current explicit producer cache size.
     */
    public int getExplicitProducerCacheSize() {
        return getConnectionFactoryProxy().getExplicitProducerCacheSize();
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
        getConnectionFactoryProxy().setExplicitProducerCacheSize(cacheSize);
    }

    /**
     * Returns the timeout to use for blocking creating new sessions
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     *
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return getConnectionFactoryProxy().getBlockIfSessionPoolIsFullTimeout();
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
        getConnectionFactoryProxy().setBlockIfSessionPoolIsFullTimeout(blockIfSessionPoolIsFullTimeout);
    }

    /**
     * Gets the configured value for the fault tolerance of pooled connections.
     *
     * @return if the pool is configured to assume connections are fault tolerant.
     */
    public boolean isFaultTolerantConnections() {
        return getConnectionFactoryProxy().isFaultTolerantConnections();
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
        getConnectionFactoryProxy().setFaultTolerantConnections(faultTolerantConnections);
    }

    //----- Connection Factory Configuration -------------------------------------------//

    /**
     * Returns the maximum number to pooled Connections that this factory will allow before it
     * begins to return existing connections from the pool on calls to ({@link #createConnection}.
     *
     * @return the maxConnections that will be created for this pool.
     */
    public int getMaxConnections() {
        return getConnectionFactoryProxy().getMaxConnections();
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
        getConnectionFactoryProxy().setMaxConnections(maxConnections);
    }

    /**
     * Gets the currently configured connection check interval for this connection factory.
     *
     * @return the number of milliseconds to sleep between runs of the connection check thread.
     */
    public long getConnectionCheckInterval() {
        return getConnectionFactoryProxy().getConnectionCheckInterval();
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
        getConnectionFactoryProxy().setConnectionCheckInterval(connectionCheckInterval);
    }

    /**
     * Checks if this pooled {@link ConnectionFactory} is creating its own {@link JMSContext} instances around
     * pooled connections or if the contexts from the configured provider are used unpooled.
     *
     * @return the true if the pool is using the provider's JMSContext instead of a pooled version.
     */
    public boolean isUseProviderJMSContext() {
        return useProviderJMSContext;
    }

    /**
     * Controls the behavior of the {@link JmsPoolAbstractConnectionFactory#createContext} methods.
     * <p>
     * By default this value is set to false and the JMS Pool will use n pooled version of
     * a JMSContext to wrap Connections from the pool.  These pooled JMSContext objects have certain
     * limitations which may not be desirable in some cases.  To use the JMSContext implementation
     * from the underlying JMS provider this option can be set to true however in that case no
     * pooling will be applied to the JMSContext's create or their underlying connections.
     *
     * @param useProviderJMSContext
     * 		Boolean value indicating whether the pool should include JMSContext in the pooling.
     */
    public void setUseProviderJMSContext(boolean useProviderJMSContext) {
        this.useProviderJMSContext = useProviderJMSContext;
    }

    //----- Internal implementation ------------------------------------------//

    /**
     * Allows subclasses to create an appropriate JmsPoolConnection wrapper for the newly
     * create connection such as one that provides support for XA Transactions.
     *
     * @param connection
     * 		The {@link JmsPoolConnection} to wrap.
     *
     * @return a new {@link JmsPoolConnection} that wraps a given {@link JmsPoolConnectionProxy}
     *
     * @throws JMSException if an error occurs while creating the new {@link Connection} instance.
     */
    protected abstract JmsPoolConnection newJmsPoolConnection(String username, String password) throws JMSException;

    /**
     * Allows subclasses to create an appropriate JmsPoolJMSContext wrapper for the newly
     * create JMSContext such as one that provides support for XA Transactions.
     *
     * @param username
     * 		The user name to use when creating the connection.
     * @param password
     * 		The password to use when creating the connection.
     * @param sessionMode
     * 		The JMS Session acknowledgement mode to use in the {@link JMSContext}
     *
     * @return a new {@link JmsPoolJMSContext} that wraps a given {@link JmsPoolConnection}
     *
     * @throws JMSException if an error occurs while creating the new {@link JMSContext} instance.
     */
    protected abstract JmsPoolJMSContext newJmsPoolContext(String username, String password, int sessionMode) throws JMSException;

    /**
     * Create a new {@link JMSContext} using the provided credentials and Session mode
     *
     * @param username
     * 		The user name to use when creating the context.
     * @param password
     * 		The password to use when creating the context.
     * @param sessionMode
     * 		The session mode to use when creating the context.
     *
     * @return a new JMSContext created using the given configuration data..
     *
     * @throws JMSRuntimeException if an error occurs while creating the new JMS Context.
     */
    protected abstract JMSContext createProviderJmsContext(String username, String password, int sessionMode);

    //----- JNDI Operations --------------------------------------------------//

    /**
     * Called by any superclass that implements a JNDI Referenceable or similar that needs to collect
     * the properties of this class for storage etc.
     *
     * This method should be updated any time there is a new property added.
     *
     * @param props
     *        a properties object that should be filled in with this objects property values.
     */
    protected void populateProperties(Properties props) {
        props.setProperty("maxSessionsPerConnection", Integer.toString(getMaxSessionsPerConnection()));
        props.setProperty("maxConnections", Integer.toString(getMaxConnections()));
        props.setProperty("connectionIdleTimeout", Integer.toString(getConnectionIdleTimeout()));
        props.setProperty("connectionCheckInterval", Long.toString(getConnectionCheckInterval()));
        props.setProperty("useAnonymousProducers", Boolean.toString(isUseAnonymousProducers()));
        props.setProperty("blockIfSessionPoolIsFull", Boolean.toString(isBlockIfSessionPoolIsFull()));
        props.setProperty("blockIfSessionPoolIsFullTimeout", Long.toString(getBlockIfSessionPoolIsFullTimeout()));
        props.setProperty("useProviderJMSContext", Boolean.toString(isUseProviderJMSContext()));
    }
}
