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

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;

/**
 * Support class that provides API for checking on specific features support
 * based on the version implemented by the provider connection that backs a
 * given instance of this type.
 */
public final class JMSVersionSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private int jmsMajorVersion = 1;
    private int jmsMinorVersion = 1;

    private final boolean sharedSubscriptions;
    private final boolean delayedDelivery;

    public JMSVersionSupport(Connection connection) {
        // Attempt to determine JMS the version support of JMS provider.
        try {
            final ConnectionMetaData connectionMetaData = connection.getMetaData();

            jmsMajorVersion = connectionMetaData.getJMSMajorVersion();
            jmsMinorVersion = connectionMetaData.getJMSMajorVersion();
        } catch (JMSException ex) {
            LOG.debug("Error while fetching JMS API version from provider, defaulting to v3.0");
            jmsMajorVersion = 3;
            jmsMinorVersion = 0;
        }

        if (isJMSVersionSupported(2, 0)) {
            sharedSubscriptions = true;
            delayedDelivery = true;
        } else {
            sharedSubscriptions = false;
            delayedDelivery = false;
        }
    }

    /**
     * Check if the JMS provider version indicates support for JMS shared subscriptions.
     *
     * @return <code>true</code> if shared subscriptions should be supported, <code>false</code> otherwise.
     */
    public boolean isSharedSubscriptionsSupported() {
        return sharedSubscriptions;
    }

    /**
     * Check if the JMS provider version indicates support for JMS delayed delivery.
     *
     * @return <code>true</code> if delayed delivery should be supported, <code>false</code> otherwise.
     */
    public boolean isDelayedDeliverySupported() {
        return delayedDelivery;
    }

    /**
     * Enforces support for JMS shared subscriptions by throwing a {@link JMSException} if the connection
     * version indicates no support is available.
     *
     * @throws JMSException if shared subscriptions are not supported.
     */
    public void enforceSharedSubscriptionSupport() throws JMSException {
        if (!sharedSubscriptions) {
            checkClientJMSVersionSupport(2, 0);
        }
    }

    /**
     * Enforces support for JMS delayed delivery by throwing a {@link JMSException} if the connection
     * version indicates no support is available.
     *
     * @throws JMSException if delayed delivery is not supported.
     */
    public void enforceDelayedDeliverySupport() throws JMSException {
        if (!delayedDelivery) {
            checkClientJMSVersionSupport(2, 0);
        }
    }

    /**
     * Enforces support for JMS Completion listener by throwing a {@link JMSException} if the connection
     * version indicates no support is available.
     *
     * @throws JMSException if completion listeners is not supported.
     */
    public void enforceCompletionListenerSupport() throws JMSException {
        if (!delayedDelivery) {
            checkClientJMSVersionSupport(2, 0);
        }
    }

    /**
     * Checks for JMS version support in the underlying JMS Connection this pooled connection
     * wrapper encapsulates.
     *
     * @param requiredMajor
     * 		The JMS Major version required for a feature to be supported.
     * @param requiredMinor
     * 		The JMS Minor version required for a feature to be supported.
     *
     * @return true if the Connection supports the version range given.
     */
    public boolean isJMSVersionSupported(int requiredMajor, int requiredMinor) {
        return jmsMajorVersion >= requiredMajor && jmsMinorVersion >= requiredMinor;
    }

    /**
     * Check if the connection that created this instance is greater than or equal to the major
     * and minor version requested.
     *
     * @param requiredMajor
     * 		The JMS Major version required for a feature to be supported.
     * @param requiredMinor
     * 		The JMS Minor version required for a feature to be supported.
     *
     * @throws JMSException if the version supported by the connection does not match or exceed the requested version.
     */
    public void checkClientJMSVersionSupport(int requiredMajor, int requiredMinor) throws JMSException {
        checkClientJMSVersionSupport(requiredMajor, requiredMinor, false);
    }

    /**
     * Check if the connection that created this instance is greater than or equal to the major
     * and minor version requested.
     *
     * @param requiredMajor
     * 		The JMS Major version required for a feature to be supported.
     * @param requiredMinor
     * 		The JMS Minor version required for a feature to be supported.
     * @param runtimeEx
     * 		Should the method throw a runtime JMS exception instead of a standard JMS exception.
     *
     * @throws JMSException if the version supported by the connection does not match or exceed the requested version.
     */
    public void checkClientJMSVersionSupport(int requiredMajor, int requiredMinor, boolean runtimeEx) throws JMSException {
        if (jmsMajorVersion >= requiredMajor && jmsMinorVersion >= requiredMinor) {
            return;
        }

        final String message = "JMS v" + requiredMajor + "." + requiredMinor + " client feature requested, " +
                               "configured client supports JMS v" + jmsMajorVersion + "." + jmsMinorVersion;

        if (runtimeEx) {
            throw new JMSRuntimeException(message);
        } else {
            throw new JMSException(message);
        }
    }
}
