/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.processor.runtime;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.Property;

class DynamicRateLimiter implements RateLimiter {
    private static final Logger logger = LoggerFactory.getLogger(DynamicRateLimiter.class);

    static class Limiter {
        final RateLimiter limiter;
        final ReentrantReadWriteLock lock;

        Limiter(RateLimiter limiter) {
            this.limiter = limiter;
            lock = new ReentrantReadWriteLock(false);
        }
    }

    private volatile Limiter current;
    private volatile boolean terminated;

    DynamicRateLimiter(Property<Long> rateProperty) {
        current = new Limiter(createLimiter(RateLimiter.UNLIMITED));

        rateProperty.listen((oldValue, newValue) -> {
            Limiter oldLimiter = current;
            current = new Limiter(createLimiter(newValue));

            boolean locked = lockExclusivelyIfNotTerminated(oldLimiter);
            try {
                oldLimiter.limiter.close();
            } catch (Exception e) {
                logger.warn("Failed to close rate limiter: {}", oldLimiter, e);
            } finally {
                if (locked) {
                    oldLimiter.lock.writeLock().unlock();
                }
            }
        });
    }

    // visible for testing
    RateLimiter createLimiter(long permitsPerSecond) {
        return RateLimiter.create(permitsPerSecond);
    }

    private boolean lockExclusivelyIfNotTerminated(Limiter limiter) {
        // If we're shutting down, we don't have to acquire exclusive lock and should go forward calling close
        // to let all its waiters to abort.
        while (!terminated) {
            try {
                if (limiter.lock.writeLock().tryLock(50, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    private Limiter readLockedLatestLimiter() {
        while (!terminated) {
            Limiter latest = current;
            if (!latest.lock.readLock().tryLock()) {
                // Failing to take read lock here means prop listener is now acquired write lock.
                // Since write lock is acquired only for closing limiter instance, there should be a new
                // limiter already prepared so its better to go grabbing a new one rather than waiting old one.
                continue;
            }
            if (latest != current) {
                // This indicates one of following two situations:
                // 1. property listener has just updated `current`, but still not have closed it because it is
                // awaiting to acquire write lock.
                // 2. property listener has updated `current` and closed it then released write lock so this
                // thread now got a read lock of it.
                //
                // In case of 2, we must retry to grab a new limiter because the `latest` has closed already.
                // In case of 1, we can use a grabbed `latest` limiter but it is preferred to retry to grab
                // a new one so the prop listener thread doesn't have to wait extra long until this thread
                // release the lock.
                continue;
            }

            // Reaching here, still prop listener might have just updated `current` with the new one, but at
            // least the currently held `latest` is guaranteed to not to be closed until this thread releases
            // the read lock, because the listener prop requires exclusive lock to close it.
            return latest;
        }

        throw new IllegalStateException("rater limiter shutdown already");
    }

    @Override
    public long acquire(int permits) throws InterruptedException {
        Limiter current = readLockedLatestLimiter();
        try {
            return current.limiter.acquire(permits);
        } finally {
            current.lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws Exception {
        Limiter current = readLockedLatestLimiter();
        try {
            // In contrast to prop listener, it is okay to close limiter here w/o write lock because that means
            // subscription is being shutdown so no new/on-going acquire() needs to success.
            terminated = true;
            current.limiter.close();
        } finally {
            current.lock.readLock().unlock();
        }
    }
}
