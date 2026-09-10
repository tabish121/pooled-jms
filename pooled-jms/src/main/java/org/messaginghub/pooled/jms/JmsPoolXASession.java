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

import javax.transaction.xa.XAResource;

import org.messaginghub.pooled.jms.internal.JmsPoolXASessionProxy;
import org.messaginghub.pooled.jms.util.JMSExceptionSupport;

import jakarta.jms.JMSException;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicSession;
import jakarta.jms.XAConnection;
import jakarta.jms.XAQueueSession;
import jakarta.jms.XASession;
import jakarta.jms.XATopicSession;

/**
 * {@link XASession} type used by JMS pool XAConnection instances.
 * <p>
 * Only JMS session instances created from an {@link XAConnection} type will implement {@link XASession}.
 */
public class JmsPoolXASession extends JmsPoolSession implements XASession, XATopicSession, XAQueueSession {

    private boolean inXATransaction;

    JmsPoolXASession(JmsPoolXASessionProxy session, boolean transactional, boolean inXATransaction) {
        super(session, transactional);

        this.inXATransaction = inXATransaction;
    }

    @Override
    public void close() throws JMSException {
        if (!inXATransaction) {
            internalClose(false);
        }
    }

    @Override
    public Session getSession() {
        return this;
    }

    @Override
    public QueueSession getQueueSession() throws JMSException {
        return this;
    }

    @Override
    public TopicSession getTopicSession() throws JMSException {
        return this;
    }

    @Override
    public XAResource getXAResource() {
        final JmsPoolXASessionProxy session;

        try {
            session = safeGetSessionProxy();
        } catch (JMSException e) {
            throw JMSExceptionSupport.createRuntimeException(e);
        }

        return session.getXAResource();
    }

    /**
     * Provides access to the wrapped JMS {@link XASession} and is meant primarily as a
     * test point and the application logic should not depend on this method.
     *
     * @return the wrapped JMS {@link XASession}.
     *
     * @throws JMSException if an error occurs while accessing the wrapped resource.
     */
    @Override
    XASession getProviderSession() throws JMSException {
        return (XASession) super.getProviderSession();
    }

    @Override
    protected JmsPoolXASessionProxy safeGetSessionProxy() throws JMSException {
        return (JmsPoolXASessionProxy) super.safeGetSessionProxy();
    }

    /**
     * Returns if the XASession was created while an XA Transaction was in effect and the
     * session should ignore any close requests other than that of the registered transaction
     * synchronization.
     *
     * @return <code>true</code> if the session is enlisted in an XA transaction.
     */
    boolean isInXATransaction() {
        return inXATransaction;
    }

    @Override
    protected boolean isRollbackOnClose() {
        return isTransactional() && !inXATransaction;
    }
}
