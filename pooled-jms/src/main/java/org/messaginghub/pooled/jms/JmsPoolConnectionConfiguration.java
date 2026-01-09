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

/**
 * Holds the immutable configuration for a JMS pool connection instance, this
 * configuration data is set at the time of the creation of a wrapped JMS connection
 * and cannot be altered during that connection's lifetime.
 */
class JmsPoolConnectionConfiguration {

    public static final int DEFAULT_MAX_SESSIONS_PER_CONNECTION = 500;
    public static final int DEFAULT_CONNECTION_IDLE_TIMEOUT = 30_000;
    public static final boolean DEFAULT_BLOCK_IF_SESSION_FULL = true;
    public static final long DEFAULT_BLOCK_IF_SESSION_FULL_TIMEOUT = -1L;
    public static final boolean DEFAULT_USE_ANONYMOUS_PRODUCERS = true;
    public static final int DEFUALT_EXPLICIT_PRODUCER_CACHE_SIZE = 0;
    public static final boolean DEFAULT_USE_PROVIDER_JMS_CONTEXT = false;
    public static final boolean DEFAULT_FAULT_TOLERANT_CONNECTIONS = false;

    // Connection specific configuration
    private int maxSessionsPerConnection = DEFAULT_MAX_SESSIONS_PER_CONNECTION;
    private int maxIdleSessionsPerConnection = DEFAULT_MAX_SESSIONS_PER_CONNECTION;
    private int connectionIdleTimeout = DEFAULT_CONNECTION_IDLE_TIMEOUT;
    private boolean blockIfSessionPoolIsFull = DEFAULT_BLOCK_IF_SESSION_FULL;
    private long blockIfSessionPoolIsFullTimeout = DEFAULT_BLOCK_IF_SESSION_FULL_TIMEOUT;
    private boolean useAnonymousProducers = DEFAULT_USE_ANONYMOUS_PRODUCERS;
    private int explicitProducerCacheSize = DEFUALT_EXPLICIT_PRODUCER_CACHE_SIZE;
    private boolean faultTolerantConnections = DEFAULT_FAULT_TOLERANT_CONNECTIONS;

    /**
     * Create a configuration based on the defined defaults.
     */
    public JmsPoolConnectionConfiguration() {}

    /**
     * Creates a copy of the provided configuration instance.
     *
     * @param configuration
     * 	The configuration to copy into the newly constructed configuration object.
     */
    public JmsPoolConnectionConfiguration(JmsPoolConnectionConfiguration configuration) {
        this.maxSessionsPerConnection = configuration.maxSessionsPerConnection;
        this.maxIdleSessionsPerConnection = configuration.maxIdleSessionsPerConnection;
        this.connectionIdleTimeout = configuration.connectionIdleTimeout;
        this.blockIfSessionPoolIsFull = configuration.blockIfSessionPoolIsFull;
        this.blockIfSessionPoolIsFullTimeout = configuration.blockIfSessionPoolIsFullTimeout;
        this.useAnonymousProducers = configuration.useAnonymousProducers;
        this.explicitProducerCacheSize = configuration.explicitProducerCacheSize;
        this.faultTolerantConnections = configuration.faultTolerantConnections;
    }

    public int getMaxIdleSessionsPerConnection() {
        return maxIdleSessionsPerConnection;
    }

    public void setMaxIdleSessionsPerConnection(int maxIdleSessionsPerConnection) {
        this.maxIdleSessionsPerConnection = maxIdleSessionsPerConnection;
    }

    public int getMaxSessionsPerConnection() {
        return maxSessionsPerConnection;
    }

    public void setMaxSessionsPerConnection(int maxSessionsPerConnection) {
        this.maxSessionsPerConnection = maxSessionsPerConnection;
    }

    public boolean isBlockIfSessionPoolIsFull() {
        return blockIfSessionPoolIsFull;
    }

    public void setBlockIfSessionPoolIsFull(boolean block) {
        this.blockIfSessionPoolIsFull = block;
    }

    public int getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    public void setConnectionIdleTimeout(int connectionIdleTimeout) {
        this.connectionIdleTimeout = connectionIdleTimeout;
    }

    public boolean isUseAnonymousProducers() {
        return useAnonymousProducers;
    }

    public void setUseAnonymousProducers(boolean anonymousProducers) {
        this.useAnonymousProducers = anonymousProducers;
    }

    public int getExplicitProducerCacheSize() {
        return explicitProducerCacheSize;
    }

    public void setExplicitProducerCacheSize(int cacheSize) {
        this.explicitProducerCacheSize = cacheSize;
    }

    public long getBlockIfSessionPoolIsFullTimeout() {
        return blockIfSessionPoolIsFullTimeout;
    }

    public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
        this.blockIfSessionPoolIsFullTimeout = blockIfSessionPoolIsFullTimeout;
    }

    public boolean isFaultTolerantConnections() {
        return faultTolerantConnections;
    }

    public void setFaultTolerantConnections(boolean faultTolerantConnections) {
        this.faultTolerantConnections = faultTolerantConnections;
    }

    /**
     * (@return an immutable snapshot of the current state of the connection configuration}
     */
    public JmsPoolConnectionConfiguration snapshot() {
        return new JmsPoolUnmodifiableConnectionConfiguration(this);
    }

    private final class JmsPoolUnmodifiableConnectionConfiguration extends JmsPoolConnectionConfiguration {

        public JmsPoolUnmodifiableConnectionConfiguration(JmsPoolConnectionConfiguration configuration) {
            super(configuration);
        }

        @Override
        public void setMaxIdleSessionsPerConnection(int maxIdleSessionsPerConnection) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setMaxSessionsPerConnection(int maxSessionsPerConnection) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setBlockIfSessionPoolIsFull(boolean block) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setConnectionIdleTimeout(int connectionIdleTimeout) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setUseAnonymousProducers(boolean anonymousProducers) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setExplicitProducerCacheSize(int cacheSize) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }

        @Override
        public void setFaultTolerantConnections(boolean faultTolerantConnections) {
            throw new UnsupportedOperationException("The connection configuration is immutable");
        }
    }
}
