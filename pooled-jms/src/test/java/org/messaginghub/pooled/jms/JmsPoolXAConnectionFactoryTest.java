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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import javax.naming.Context;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.messaginghub.pooled.jms.mock.MockJMSInitialContextFactory;
import org.messaginghub.pooled.jms.mock.MockJMSXAConnectionFactory;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import jakarta.jms.Session;
import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * Tests for the pooled XA connection factory configuration, with an emphasis on how the
 * {@link TransactionManager} used to enlist pooled sessions is resolved and handed to the
 * connections the factory creates.
 * <p>
 * An EE deployment configures the factory with {@code tmFromJndi} and a JNDI name instead of
 * injecting a {@link TransactionManager} instance, in which case the TM is resolved lazily from
 * JNDI. These tests install {@link MockJMSInitialContextFactory} so that the JNDI lookup performed
 * by the factory can be exercised without a real JNDI provider.
 */
class JmsPoolXAConnectionFactoryTest extends JmsPoolTestSupport {

    private static final String TM_JNDI_NAME = "java:/TransactionManager";

    private JmsPoolXAConnectionFactory xaCF;

    private MockJMSXAConnectionFactory xaFactory;

    private String originalJndiInitialContextFactory;

    @Mock
    TransactionManager txManager;

    @Mock
    Transaction txn;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        MockitoAnnotations.openMocks(this);

        // A mock reports STATUS_NO_TRANSACTION (zero) by default which would make the pool treat
        // every session as non-XA no matter how the connection factory was configured, so make
        // the mock report an active global transaction for these tests.
        when(txManager.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(txManager.getTransaction()).thenReturn(txn);
        when(txn.enlistResource(any())).thenReturn(true);

        xaFactory = new MockJMSXAConnectionFactory();

        xaCF = new JmsPoolXAConnectionFactory();
        xaCF.setConnectionFactory(xaFactory);
        xaCF.setMaxConnections(1);

        originalJndiInitialContextFactory = System.getProperty(Context.INITIAL_CONTEXT_FACTORY);
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, MockJMSInitialContextFactory.class.getName());
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        try {
            xaCF.stop();
        } catch (Exception ex) {
            // ignored
        } finally {
            if (originalJndiInitialContextFactory != null) {
                System.setProperty(Context.INITIAL_CONTEXT_FACTORY, originalJndiInitialContextFactory);
            } else {
                System.clearProperty(Context.INITIAL_CONTEXT_FACTORY);
            }

            // The mock registry is shared for the lifetime of the test JVM.
            MockJMSInitialContextFactory.clear();
        }

        super.tearDown();
    }

    //----- TransactionManager resolved from JNDI ------------------------------------//

    @Test
    public void testTransactionManagerResolvedFromJndi() throws Exception {
        configureForJndiTransactionManager();

        assertSame(txManager, xaCF.getTransactionManager(), "Should have resolved the TM bound in JNDI");
    }

    @Test
    public void testCreateConnectionUsesTransactionManagerResolvedFromJndi() throws Exception {
        configureForJndiTransactionManager();

        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection();
        Session session = connection.createSession();

        assertNotNull(session);

        assertSessionEnlistedInGlobalTransaction();

        assertEquals(0, connection.getNumIdleSessions());
        session.close();

        // Session is owned by the global transaction so it ignores the user close.
        assertEquals(0, connection.getNumIdleSessions());
    }

    @Test
    public void testCreateConnectionWithCredentialsUsesTransactionManagerResolvedFromJndi() throws Exception {
        configureForJndiTransactionManager();

        // Connections are pooled per user name and password so check the create with credentials
        // path resolves the TM too and not just the anonymous one.
        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection("user", "password");
        assertNotNull(connection.createSession());

        assertSessionEnlistedInGlobalTransaction();
    }

    @Test
    public void testCreateXAConnectionUsesTransactionManagerResolvedFromJndi() throws Exception {
        configureForJndiTransactionManager();

        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createXAConnection();
        assertNotNull(connection.createSession());

        assertSessionEnlistedInGlobalTransaction();
    }

    @Test
    public void testCreateConnectionUsesTransactionManagerResolvedFromJndiForNamedLookup() throws Exception {
        // The TM lookup must honor a configured JNDI name rather than only the default.
        MockJMSInitialContextFactory.bind("java:/comp/env/TransactionManager", txManager);

        xaCF.setTmJndiName("java:/comp/env/TransactionManager");
        xaCF.setTmFromJndi(true);

        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection();
        assertNotNull(connection.createSession());

        assertSessionEnlistedInGlobalTransaction();
    }

    //----- TransactionManager assigned directly -------------------------------------//

    @Test
    public void testCreateConnectionUsesDirectlyAssignedTransactionManager() throws Exception {
        // Control case: the assignment based configuration must keep working, and it proves the
        // enlistment assertions used above are capable of passing.
        xaCF.setTransactionManager(txManager);

        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection();
        assertNotNull(connection.createSession());

        assertSessionEnlistedInGlobalTransaction();
    }

    @Test
    public void testCreateConnectionUsesTransactionManagerResolvedFromJndiWhenAlsoAssigned() throws Exception {
        configureForJndiTransactionManager();
        xaCF.setTransactionManager(txManager);

        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection();
        assertNotNull(connection.createSession());

        assertSessionEnlistedInGlobalTransaction();
    }

    //----- No TransactionManager available -------------------------------------------//

    @Test
    public void testCreateConnectionWithoutAnyTransactionManagerDoesNotEnlist() throws Exception {
        // Documents the degraded behaviour a pooled XA connection falls back to when no TM can be
        // resolved at all: sessions are handed out usable but are never enlisted, so a rollback of
        // the surrounding global transaction does not undo the JMS work. An unresolvable TM must
        // never look like a successfully configured XA connection.
        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection();
        assertNotNull(connection.createSession());

        verifyNoInteractions(txManager);
        verifyNoInteractions(txn);
    }

    @Test
    public void testCreateConnectionWithUnresolvableJndiTransactionManagerDoesNotEnlist() throws Exception {
        // Nothing is bound for this name so the lookup fails and the pool has no TM to work with.
        xaCF.setTmJndiName("java:/MissingTransactionManager");
        xaCF.setTmFromJndi(true);

        JmsPoolXAConnection connection = (JmsPoolXAConnection) xaCF.createConnection();
        assertNotNull(connection.createSession());

        verifyNoInteractions(txManager);
        verifyNoInteractions(txn);
    }

    //----- Internal helpers ----------------------------------------------------------//

    /**
     * Configures the factory the way an EE deployment would, with the TM resolved from JNDI on
     * demand rather than a TM instance assigned to the factory.
     */
    private void configureForJndiTransactionManager() {
        MockJMSInitialContextFactory.bind(TM_JNDI_NAME, txManager);

        xaCF.setTmJndiName(TM_JNDI_NAME);
        xaCF.setTmFromJndi(true);
    }

    /**
     * Verifies that the pooled connection actually consulted the resolved TM and enlisted the
     * pooled session in the global transaction. Without these interactions the session runs
     * outside of the global transaction and its work is not rolled back with it.
     */
    private void assertSessionEnlistedInGlobalTransaction() throws Exception {
        verify(txManager, atLeastOnce()).getStatus();
        verify(txn).registerSynchronization(any());
        verify(txn).enlistResource(any());
    }
}
