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
package org.messaginghub.pooled.jms.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;

/**
 * Tests for the JMS version support utility
 */
public class JMSVersionSupportTest {

    protected Connection mockConnection;
    protected ConnectionMetaData mockMetaData;

    @BeforeEach
    public void setUp() throws JMSException {
        mockConnection = Mockito.mock(Connection.class);
        mockMetaData = Mockito.mock(ConnectionMetaData.class);

        Mockito.when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    }

    @Test
    public void testDefaultsToV3IfMetaThrowsForMajorVersion() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMajorVersion()).thenThrow(JMSException.class);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertTrue(vs.isDelayedDeliverySupported());
        assertTrue(vs.isSharedSubscriptionsSupported());
        assertTrue(vs.isJMSVersionSupported(3, 0));
    }

    @Test
    public void testDefaultsToV3IfMetaThrowsForMinorVersion() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMinorVersion()).thenThrow(JMSException.class);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertTrue(vs.isDelayedDeliverySupported());
        assertTrue(vs.isSharedSubscriptionsSupported());
        assertTrue(vs.isJMSVersionSupported(3, 0));
    }

    @Test
    public void testLowerJMSVersionDisablesFeatures() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMajorVersion()).thenReturn(1);
        Mockito.when(mockConnection.getMetaData().getJMSMinorVersion()).thenReturn(1);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertFalse(vs.isDelayedDeliverySupported());
        assertFalse(vs.isSharedSubscriptionsSupported());
        assertFalse(vs.isJMSVersionSupported(3, 0));

        assertThrows(JMSException.class, () -> vs.enforceCompletionListenerSupport());
        assertThrows(JMSException.class, () -> vs.enforceDelayedDeliverySupport());
        assertThrows(JMSException.class, () -> vs.enforceSharedSubscriptionSupport());
    }

    @Test
    public void testJMSVersionAtTwoEnablesFeatures() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMajorVersion()).thenReturn(2);
        Mockito.when(mockConnection.getMetaData().getJMSMinorVersion()).thenReturn(0);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertTrue(vs.isDelayedDeliverySupported());
        assertTrue(vs.isSharedSubscriptionsSupported());
        assertTrue(vs.isJMSVersionSupported(2, 0));

        assertDoesNotThrow(() -> vs.enforceCompletionListenerSupport());
        assertDoesNotThrow(() -> vs.enforceDelayedDeliverySupport());
        assertDoesNotThrow(() -> vs.enforceSharedSubscriptionSupport());
    }

    @Test
    public void testJMSVersionSupportedForNonMatchingVersions() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMajorVersion()).thenReturn(4);
        Mockito.when(mockConnection.getMetaData().getJMSMinorVersion()).thenReturn(0);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertFalse(vs.isJMSVersionSupported(5, 0));
        assertFalse(vs.isJMSVersionSupported(4, 1));

        assertTrue(vs.isJMSVersionSupported(3, 0));
        assertTrue(vs.isJMSVersionSupported(3, 1));
    }

    @Test
    public void testCheckJMSVersionSupportedForNonMatchingVersions() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMajorVersion()).thenReturn(4);
        Mockito.when(mockConnection.getMetaData().getJMSMinorVersion()).thenReturn(0);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertDoesNotThrow(() -> vs.checkClientJMSVersionSupport(3, 0));

        assertThrows(JMSException.class, () -> vs.checkClientJMSVersionSupport(5, 0));
        assertThrows(JMSException.class, () -> vs.checkClientJMSVersionSupport(4, 1));
    }

    @Test
    public void testCheckJMSVersionSupportedForNonMatchingVersionsRE() throws JMSException {
        Mockito.when(mockConnection.getMetaData().getJMSMajorVersion()).thenReturn(4);
        Mockito.when(mockConnection.getMetaData().getJMSMinorVersion()).thenReturn(0);

        final JMSVersionSupport vs = new JMSVersionSupport(mockConnection);

        assertDoesNotThrow(() -> vs.checkClientJMSVersionSupport(3, 0, true));

        assertThrows(JMSRuntimeException.class, () -> vs.checkClientJMSVersionSupport(5, 0, true));
        assertThrows(JMSRuntimeException.class, () -> vs.checkClientJMSVersionSupport(4, 1, true));
    }
}
