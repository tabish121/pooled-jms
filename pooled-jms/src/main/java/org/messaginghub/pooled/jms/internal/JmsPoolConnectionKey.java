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

import java.util.Objects;

/**
 * A cache key for the connection details
 */
final class JmsPoolConnectionKey {

    private final String username;
    private final String password;
    private final int hashCode;

    /**
     * Creates a new ConnectionKey using the supplied values.
     *
     * @param username
     * 		The user name that this key represents
     * @param password
     * 		The password that this key represents
     */
    public JmsPoolConnectionKey(String username, String password) {
        this.password = password;
        this.username = username;
        this.hashCode = computeHash(username, password);
    }

    public String getPassword() {
        return password;
    }

    public String getUserName() {
        return username;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof JmsPoolConnectionKey key) {
            if (hashCode != key.hashCode) {
                return false;
            }

            if (!Objects.equals(password, key.password)) {
                return false;
            }

            if (!Objects.equals(username, key.username)) {
                return false;
            }

            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + username + " }";
    }

    private static int computeHash(String username, String password) {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }
}
