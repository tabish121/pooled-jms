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
package org.messaginghub.pooled.jms.mock;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/**
 * Mock JNDI {@link InitialContextFactory} for tests that need {@code new InitialContext()} to
 * resolve bound objects without a real JNDI provider being available on the test classpath.
 * <p>
 * The factory is installed by assigning {@link Context#INITIAL_CONTEXT_FACTORY} to the name of
 * this class which makes the no-arg {@link javax.naming.InitialContext} constructor create
 * instances of it. Bound objects are kept in a static registry because the JNDI lookup is
 * performed from within the code under test which has no way to be given a context directly.
 * <p>
 * The registry is shared for the lifetime of the test JVM so tests must {@link #clear()} it when
 * they are done to avoid leaking bindings into unrelated tests, the suite runs with a single
 * reused fork and in random order.
 */
public class MockJMSInitialContextFactory implements InitialContextFactory {

    private static final Map<String, Object> BOUND_OBJECTS = new ConcurrentHashMap<>();

    /**
     * Binds the given object into the mock registry under the provided name so that a subsequent
     * lookup of that name from the created initial context returns it.
     *
     * @param name
     *      The JNDI name the object should be resolvable under.
     * @param object
     *      The object to return for a lookup of the given name.
     */
    public static void bind(String name, Object object) {
        BOUND_OBJECTS.put(name, object);
    }

    /**
     * Removes every previously bound object from the shared mock registry.
     */
    public static void clear() {
        BOUND_OBJECTS.clear();
    }

    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        final Context context = mock(Context.class);

        when(context.lookup(anyString())).thenAnswer(invocation -> BOUND_OBJECTS.get(invocation.getArgument(0)));

        return context;
    }
}
