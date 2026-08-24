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

import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.messaginghub.pooled.jms.internal.JmsPoolQueueBrowserProxy;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;

/**
 * A {@link QueueBrowser} which was created by {@link JmsPoolSession}.
 */
public class JmsPoolQueueBrowser implements QueueBrowser, AutoCloseable {

    private static final AtomicIntegerFieldUpdater<JmsPoolQueueBrowser> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolQueueBrowser.class, "closed");

    private final JmsPoolQueueBrowserProxy queueBrowser;
    private final Consumer<JmsPoolQueueBrowser> onClose;

    private volatile int closed;

    JmsPoolQueueBrowser(JmsPoolQueueBrowserProxy delegate, Consumer<JmsPoolQueueBrowser> onClose) {
        this.queueBrowser = delegate;
        this.onClose = onClose;
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkClosed();
        return queueBrowser.getQueue();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return queueBrowser.getMessageSelector();
    }

    @Override
    public Enumeration<?> getEnumeration() throws JMSException {
        checkClosed();
        return queueBrowser.getEnumeration();
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            try {
                queueBrowser.close();
            } finally {
                onClose.accept(this);
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + queueBrowser + " }";
    }

    /**
     * Provides access to the wrapped JMS {@link QueueBrowser} and is meant primarily as a
     * test point and the application logic should not depend on this method.
     *
     * @return the wrapped JMS {@link QueueBrowser}.
     *
     * @throws JMSException if an error occurs while accessing the wrapped resource.
     */
    QueueBrowser getProviderQueueBrowser() throws JMSException {
        checkClosed();
        return queueBrowser.getProviderQueueBrowser();
    }

    private void checkClosed() throws IllegalStateException {
        if (closed != 0) {
            throw new IllegalStateException("The QueueBrowser is closed");
        }
    }
}
