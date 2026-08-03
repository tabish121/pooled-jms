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

import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.xa.XAResource;

import org.messaginghub.pooled.jms.internal.JmsPoolXAConnectionProxy;
import org.messaginghub.pooled.jms.internal.JmsPoolXASessionProxy;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.XAConnection;
import jakarta.jms.XAQueueConnection;
import jakarta.jms.XAQueueSession;
import jakarta.jms.XASession;
import jakarta.jms.XATopicConnection;
import jakarta.jms.XATopicSession;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * {@link XAConnection} pooled connection wrapper for shared connections from the pool.
 */
public class JmsPoolXAConnection extends JmsPoolConnection implements XAConnection, XATopicConnection, XAQueueConnection, AutoCloseable {

    private final TransactionManager transactionManager;

    JmsPoolXAConnection(JmsPoolXAConnectionProxy connection, TransactionManager transactionManager) {
        super(connection);

        this.transactionManager = transactionManager;
    }

    @Override
    public XAQueueSession createXAQueueSession() throws JMSException {
        return (XAQueueSession) createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public XATopicSession createXATopicSession() throws JMSException {
        return (XATopicSession) createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public XASession createXASession() throws JMSException {
        return createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Provides access to the wrapped JMS {@link XAConnection} and is meant primarily as a
     * test point and the application logic should not depend on this method.
     *
     * @return the wrapped JMS {@link XAConnection}.
     *
     * @throws JMSException if an error occurs while accessing the wrapped resource.
     */
    @Override
    XAConnection getProviderConnection() throws JMSException {
        return (XAConnection) super.getProviderConnection();
    }

    @Override
    public XASession createSession(boolean transacted, int ackMode) throws JMSException {
        try {
            final boolean isXa = (transactionManager != null && transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION);

            if (isXa) {
                // if the xa tx aborts inflight we don't want to auto create a
                // local transaction or auto ack
                transacted = false;
                ackMode = Session.CLIENT_ACKNOWLEDGE;
            } else if (transactionManager != null) {
                // cmt or transactionManager managed
                transacted = false;
                if (ackMode == Session.SESSION_TRANSACTED) {
                    ackMode = Session.AUTO_ACKNOWLEDGE;
                }
            }

            final JmsPoolXASessionProxy proxy = safeGetConnection().createSession(transacted, ackMode);
            final JmsPoolXASession session = afterSessionCreated(new JmsPoolXASession(proxy, transacted, isXa));

            if (isXa) {
                // Register a new reference on the active connection such that even if it this connection
                // is closed by the user the underlying connection remains active until the synchronization
                // indicates the transaction is complete regardless of its outcome.
                safeGetConnection().acquire();

                final JmsPooledXASessionSynchronization sync = new JmsPooledXASessionSynchronization(session);

                try {
                    final Transaction txn = transactionManager.getTransaction();

                    txn.registerSynchronization(sync);

                    if (!txn.enlistResource(createXaResource(session))) {
                        throw new JMSException("Enlistment of Pooled Session into transaction failed");
                    }
                } catch (Exception ex) {
                    sync.fail();
                    throw ex;
                }
            }

            return session;
        } catch (RollbackException e) {
            final JMSException jmsException = new JMSException("Rollback Exception");
            jmsException.initCause(e);
            throw jmsException;
        } catch (SystemException e) {
            final JMSException jmsException = new JMSException("System Exception");
            jmsException.initCause(e);
            throw jmsException;
        }
    }

    @Override
    protected JmsPoolXAConnectionProxy safeGetConnection() throws JMSException {
        return (JmsPoolXAConnectionProxy) super.safeGetConnection();
    }

    @Override
    protected JmsPoolXASession afterSessionCreated(JmsPoolSession session) throws JMSException {
        return (JmsPoolXASession) super.afterSessionCreated(session);
    }

    protected XAResource createXaResource(JmsPoolXASession session) throws JMSException {
        return session.getXAResource();
    }

    protected class JmsPooledXASessionSynchronization implements jakarta.transaction.Synchronization {

        private final AtomicBoolean closed = new AtomicBoolean();

        private JmsPoolXASession session;

        private JmsPooledXASessionSynchronization(JmsPoolXASession session) {
            this.session = session;
        }

        public void fail() throws JMSException {
            if (closed.compareAndSet(false, true)) {
                // Force the session to close and invalidate itself.
                try {
                    session.internalClose(true);
                } finally {
                    session = null;
                    safeGetConnection().close();
                }
            }
        }

        public void close() throws JMSException {
            if (closed.compareAndSet(false, true)) {
                try {
                    session.internalClose(false);
                } finally {
                    session = null;
                    safeGetConnection().close();
                }
            }
        }

        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            try {
                close();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
