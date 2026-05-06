/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;

/**
 * Handle locks by key.
 *
 * <p>The implementation is tuned for the hot path of acquiring an existing
 * lock entry: it uses a lock-free fast path that does only a {@code get} on
 * the underlying {@link ConcurrentHashMap} plus a CAS on an
 * {@link AtomicInteger} reference counter. Only when the key is absent or
 * the existing entry is being concurrently retired does the code fall back
 * to the slower {@link ConcurrentHashMap#compute compute} path that takes
 * the per-bin lock.
 *
 * <p>Allocation behaviour:
 * <ul>
 *   <li>The slow-path {@link BiFunction} is a {@code static final} singleton
 *       that captures no state, so it does not allocate per call.</li>
 *   <li>A new {@link LockInstance} (and its {@link StampedLock}) is
 *       allocated only when the key is genuinely new in the map (or is
 *       being resurrected after the previous holder dropped the refcount
 *       to zero).</li>
 *   <li>The release path is fully allocation-free in the common case
 *       (refcount goes from {@code N} to {@code N-1}) and uses an
 *       allocation-free {@link ConcurrentHashMap#remove(Object, Object)}
 *       CAS-remove when the refcount drops to zero.</li>
 * </ul>
 *
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class LocalLockManager implements ILocalLockManager {

    private int writeLockTimeout = 60 * 30;

    private int readLockTimeout = 60 * 30;

    private final ConcurrentMap<Bytes, LockInstance> locks = new ConcurrentHashMap<>();

    /**
     * State of a single per-key lock.
     *
     * <p>{@link #refCount} is incremented every time a thread joins the
     * lock instance (either by creating it or by attaching to an existing
     * one) and decremented on release. While {@code refCount} is greater
     * than zero the instance is "alive" and reachable through the fast
     * path. When it reaches zero the instance is retired: no thread can
     * resurrect a refcount of zero through the fast path, and the slow
     * path detects the zero and replaces the dead instance with a fresh
     * one under the bin lock.
     */
    private static final class LockInstance {

        private final StampedLock lock;
        private final AtomicInteger refCount;

        LockInstance() {
            this.lock = new StampedLock();
            // Newly created instances start with one outstanding reference,
            // owned by the thread that created them.
            this.refCount = new AtomicInteger(1);
        }

        @Override
        public String toString() {
            return "LockInstance{lock=" + lock + ", refCount=" + refCount.get() + '}';
        }

    }

    /**
     * Slow-path acquire function used inside
     * {@link ConcurrentHashMap#compute compute}. Either creates a brand-new
     * {@link LockInstance} (when the key is absent or the existing entry
     * has been retired with {@code refCount == 0}) or atomically increments
     * the refcount on the existing entry. Implemented as a {@code static
     * final} field so it is allocated exactly once per JVM and incurs no
     * per-call lambda allocation.
     */
    private static final BiFunction<Bytes, LockInstance, LockInstance> ACQUIRE_FN =
            (k, existing) -> {
                if (existing == null) {
                    return new LockInstance();
                }
                final AtomicInteger ref = existing.refCount;
                while (true) {
                    final int c = ref.get();
                    if (c == 0) {
                        // The previous holder has already decremented to
                        // zero and is about to remove the entry. Replace
                        // it now with a fresh instance under the bin
                        // lock; the pending remove will see the new
                        // instance, fail the CAS-remove, and become a
                        // no-op.
                        return new LockInstance();
                    }
                    if (ref.compareAndSet(c, c + 1)) {
                        return existing;
                    }
                }
            };

    private LockInstance makeLockForKey(Bytes key) {
        // Fast path: the key is already in the map and is alive
        // (refCount > 0). Bump the refcount with a CAS — no bin lock,
        // no allocation, no lambda capture.
        final LockInstance existing = locks.get(key);
        if (existing != null) {
            final AtomicInteger ref = existing.refCount;
            while (true) {
                final int c = ref.get();
                if (c == 0) {
                    // Being retired by a concurrent release; fall through
                    // to the slow path which will resurrect the entry
                    // under the bin lock.
                    break;
                }
                if (ref.compareAndSet(c, c + 1)) {
                    return existing;
                }
            }
        }
        // Slow path: key is absent or its current entry is being retired.
        return locks.compute(key, ACQUIRE_FN);
    }

    private void returnLockForKey(LockInstance instance, Bytes key) throws IllegalStateException {
        final int c = instance.refCount.decrementAndGet();
        if (c < 0) {
            // Programming error: more releases than acquires (e.g. double
            // release of the same handle).
            throw new IllegalStateException("trying to release un-owned lock (refCount underflow)");
        }
        if (c == 0) {
            // Last reference. Try to remove the entry; the CAS-remove
            // is a no-op if the slow-path acquire has already replaced
            // it with a fresh instance, which is the expected outcome
            // of a resurrection race and not an error.
            locks.remove(key, instance);
        }
    }

    public int getWriteLockTimeout() {
        return writeLockTimeout;
    }

    public void setWriteLockTimeout(int writeLockTimeout) {
        this.writeLockTimeout = writeLockTimeout;
    }

    public int getReadLockTimeout() {
        return readLockTimeout;
    }

    public void setReadLockTimeout(int readLockTimeout) {
        this.readLockTimeout = readLockTimeout;
    }

    @Override
    public LockHandle acquireWriteLockForKey(Bytes key) {
        LockInstance lock = makeLockForKey(key);
        try {
            long tryWriteLock = lock.lock.tryWriteLock(writeLockTimeout, TimeUnit.SECONDS);
            if (tryWriteLock == 0) {
                // We bumped the refcount but never acquired the StampedLock;
                // give the reference back so the entry can be retired.
                returnLockForKey(lock, key);
                throw new LockAcquireTimeoutException("timed out acquiring lock for write");
            }
            return new LockHandle(tryWriteLock, key, true, lock);
        } catch (InterruptedException err) {
            // Same cleanup as the timeout path: we hold a refcount but no
            // StampedLock stamp.
            returnLockForKey(lock, key);
            Thread.currentThread().interrupt();
            throw new LockAcquireTimeoutException(err);
        }
    }

    @Override
    public void releaseWriteLock(LockHandle handle) {
        /* Retrieve the instance... other threads could have this pointer too */
        LockInstance instance = (LockInstance) handle.handle;
        instance.lock.unlockWrite(handle.stamp);
        returnLockForKey(instance, handle.key);
    }

    @Override
    public LockHandle acquireReadLockForKey(Bytes key) {
        LockInstance lock = makeLockForKey(key);
        try {
            long tryReadLock = lock.lock.tryReadLock(readLockTimeout, TimeUnit.SECONDS);
            if (tryReadLock == 0) {
                // We bumped the refcount but never acquired the StampedLock;
                // give the reference back so the entry can be retired.
                returnLockForKey(lock, key);
                throw new LockAcquireTimeoutException("timedout trying to read lock");
            }
            return new LockHandle(tryReadLock, key, false, lock);
        } catch (InterruptedException err) {
            // Same cleanup as the timeout path.
            returnLockForKey(lock, key);
            Thread.currentThread().interrupt();
            throw new LockAcquireTimeoutException(err);
        }
    }

    @Override
    public void releaseReadLock(LockHandle handle) {
        LockInstance instance = (LockInstance) handle.handle;
        instance.lock.unlockRead(handle.stamp);
        returnLockForKey(instance, handle.key);
    }

    @Override
    public void releaseLock(LockHandle handle) {
        if (handle == null) {
            return;
        }
        if (handle.write) {
            releaseWriteLock(handle);
        } else {
            releaseReadLock(handle);
        }
    }

    @Override
    public void clear() {
        this.locks.clear();
    }

    @Override
    public int getNumKeys() {
        return locks.size();
    }

}
