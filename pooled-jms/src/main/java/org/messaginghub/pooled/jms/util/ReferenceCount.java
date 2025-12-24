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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A thread safe reference counter for use in JMS pooled resources to track the
 * number of references to a shared resource.
 */
public final class ReferenceCount {

    private static final AtomicIntegerFieldUpdater<ReferenceCount> COUNT_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ReferenceCount.class, "count");

    private final int minValue;
    private volatile int count;

    public ReferenceCount() {
        this(0);
    }

    /**
     * Create a reference counter with a minimum value which controls the floor
     * of the counter when calling the {@link #decrement()} method.
     *
     * @param minValue
     * 	The floor value the reference count can fall to (cannot be negative).
     */
    public ReferenceCount(int minValue) {
        if (minValue < 0) {
            throw new IllegalArgumentException("Reference counter does not accept negative minimum values");
        }

        this.minValue = minValue;
        this.count = minValue;
    }

    public int getCount() {
        return count;
    }

    public int decrement() {
        return COUNT_UPDATER.accumulateAndGet(this, -1, this::referenceCountUpdater);
    }

    public int increment() {
        return COUNT_UPDATER.accumulateAndGet(this, 1, this::referenceCountUpdater);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + count;
    }

    private int referenceCountUpdater(int previous, int addition) {
        final long newCount = (long) previous + addition;

        if (newCount > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else if (newCount < minValue) {
            return minValue;
        } else {
            return (int) newCount;
        }
    }
}
