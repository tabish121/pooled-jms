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
 * A reference counted type used when an objects lifetime depends on some other
 * object retains a reference here.
 */
public final class ReferenceCounted implements Referenced {

    private static final AtomicIntegerFieldUpdater<ReferenceCounted> COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ReferenceCounted.class, "count");

    private volatile int count;

    @Override
    public ReferenceCounted acquire() {
        COUNT_UPDATER.accumulateAndGet(this, 1, this::referenceCountUpdater);
        return this;
    }

    @Override
    public boolean release() {
        return COUNT_UPDATER.accumulateAndGet(this, -1, this::referenceCountUpdater) == 0;
    }

    @Override
    public boolean isReferenced() {
        return count != 0;
    }

    @Override
    public boolean isUnreferenced() {
        return count == 0;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":=" + count;
    }

    private int referenceCountUpdater(int previous, int addition) {
        final long newCount = (long) previous + addition;

        if (newCount > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else if (newCount < 0) {
            return 0;
        } else {
            return (int) newCount;
        }
    }
}
