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

    private final JmsPoolSession session;
    private final QueueBrowser delegate;
    private volatile int closed;

    /**
     * Wraps the QueueBrowser.
     *
     * @param session
     * 		the pooled session that created this object.
     * @param delegate
     * 		the created QueueBrowser to wrap.
     */
    public JmsPoolQueueBrowser(JmsPoolSession session, QueueBrowser delegate) {
        this.session = session;
        this.delegate = delegate;
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkClosed();
        return delegate.getQueue();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return delegate.getMessageSelector();
    }

    @Override
    public Enumeration<?> getEnumeration() throws JMSException {
        checkClosed();
        return delegate.getEnumeration();
    }

    @Override
    public void close() throws JMSException {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            // ensure session removes browser from it's list of managed resources.
            session.onQueueBrowserClose(this);
            delegate.close();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + getDelegate() + " }";
    }

    public QueueBrowser getQueueBrowser() throws JMSException {
        checkClosed();
        return delegate;
    }

    /**
     * @return the underlying {@link QueueBrowser} that this wrapper object is a proxy to.
     */
    QueueBrowser getDelegate() {
        return delegate;
    }

    private void checkClosed() throws IllegalStateException {
        if (closed != 0) {
            throw new IllegalStateException("The QueueBrowser is closed");
        }
    }
}
