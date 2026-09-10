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

import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;

public final class JmsPoolQueueBrowserProxy implements QueueBrowser {

    private static final AtomicIntegerFieldUpdater<JmsPoolQueueBrowserProxy> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(JmsPoolQueueBrowserProxy.class, "closed");

    private final QueueBrowser browser;

    private volatile int closed;

    JmsPoolQueueBrowserProxy(QueueBrowser browser) {
        this.browser = browser;
    }

    public QueueBrowser getProviderQueueBrowser() throws JMSException {
        checkClosed();
        return browser;
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkClosed();
        return browser.getQueue();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return browser.getMessageSelector();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getEnumeration() throws JMSException {
        checkClosed();
        return browser.getEnumeration();
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            browser.close();
        }
    }

    /**
     * Gets the state of the queue browser closed flag.
     *
     * @return <code>true</code> if the queue browser has been closed.
     */
    protected boolean isClosed() {
        return closed > 0;
    }

    /**
     * Checks for closure of this queue browser wrapper and throws if true.
     *
     * @throws IllegalStateException if the queue browser is closed.
     */
    protected void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("The queue browser has already been closed");
        }
    }
}
