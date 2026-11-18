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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import jakarta.jms.ConnectionConsumer;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.ServerSessionPool;

/**
 * Wrapper around a JMS {@link ConnectionConsumer} which can allow the pooled connections
 * to clean up the connection consumers created by each shared connection as it closes.
 */
public class JmsPoolConnectionConsumer implements ConnectionConsumer {

    private static final AtomicIntegerFieldUpdater<JmsPoolConnectionConsumer> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolConnectionConsumer.class, "closed");

    private final Consumer<JmsPoolConnectionConsumer> onClose;
    private final ConnectionConsumer consumer;
    private final Destination destination;

    private volatile int closed;

    JmsPoolConnectionConsumer(ConnectionConsumer consumer, Destination destination, Consumer<JmsPoolConnectionConsumer> onClose) {
        this.consumer = consumer;
        this.destination = destination;
        this.onClose = onClose;
    }

    @Override
    public ServerSessionPool getServerSessionPool() throws JMSException {
        checkClosed();
        return consumer.getServerSessionPool();
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            try {
                // As with any JMS resource, a failure on close doesn't have a real
                // path to recovery. We pass along any exception here but consider this
                // as our attempt to close and if the provider did not close itself then
                // that is left as an issue in the provider instance.
                consumer.close();
            } finally {
                onClose.accept(this);
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { destination=" + destination + " }";
    }

    /**
     * Checks if the wrapper has been closed previously.
     *
     * @throws JMSException if the wrapper was closed.
     */
    protected void checkClosed() throws JMSException {
        if (closed != 0) {
            throw new IllegalStateException("The MessageConsumer is closed");
        }
    }
}
