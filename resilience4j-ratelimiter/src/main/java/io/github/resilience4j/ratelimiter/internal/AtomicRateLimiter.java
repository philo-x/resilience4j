/*
 *
 *  Copyright 2016 Robert Winkler and Bohdan Storozhuk
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.ratelimiter.internal;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnFailureEvent;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnSuccessEvent;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static java.lang.Long.min;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * {@link AtomicRateLimiter} splits all nanoseconds from the start of epoch into cycles.
 * <p>Each cycle has duration of {@link RateLimiterConfig#limitRefreshPeriod} in nanoseconds.
 * <p>By contract on start of each cycle {@link AtomicRateLimiter} should
 * set {@link State#activePermissions} to {@link RateLimiterConfig#limitForPeriod}.
 * For the {@link AtomicRateLimiter} callers it is really looks so, but under the hood there is
 * some optimisations that will skip this refresh if {@link AtomicRateLimiter} is not used actively.
 * <p>All {@link AtomicRateLimiter} updates are atomic and state is encapsulated in {@link AtomicReference} to
 * {@link AtomicRateLimiter.State}
 */
public class AtomicRateLimiter implements RateLimiter {
    // 记录限流器启动时系统时间(纳秒)
    private static final long nanoTimeStart = nanoTime();
    // 限流器名称
    private final String name;
    // 等待的线程数
    private final AtomicInteger waitingThreads;
    // 限流器状态
    private final AtomicReference<State> state;
    // 事件处理器
    private final RateLimiterEventProcessor eventProcessor;


    public AtomicRateLimiter(String name, RateLimiterConfig rateLimiterConfig) {
        this.name = name;

        waitingThreads = new AtomicInteger(0);
        state = new AtomicReference<>(new State(
                rateLimiterConfig, 0, rateLimiterConfig.getLimitForPeriod(), 0
        ));
        eventProcessor = new RateLimiterEventProcessor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeTimeoutDuration(final Duration timeoutDuration) {
        RateLimiterConfig newConfig = RateLimiterConfig.from(state.get().config)
                .timeoutDuration(timeoutDuration)
                .build();
        state.updateAndGet(currentState -> new State(
                newConfig, currentState.activeCycle, currentState.activePermissions, currentState.nanosToWait
        ));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeLimitForPeriod(final int limitForPeriod) {
        RateLimiterConfig newConfig = RateLimiterConfig.from(state.get().config)
                .limitForPeriod(limitForPeriod)
                .build();
        state.updateAndGet(currentState -> new State(
                newConfig, currentState.activeCycle, currentState.activePermissions, currentState.nanosToWait
        ));
    }

    /**
     * Calculates time elapsed from the class loading.
     */
    private long currentNanoTime() {
        return nanoTime() - nanoTimeStart;
    }

    /**
     * {@inheritDoc}
     * 令牌桶的大小由超时时间决定。
     */
    @Override
    public boolean getPermission(final Duration timeoutDuration) {
        long timeoutInNanos = timeoutDuration.toNanos();
        // 状态计算及转换
        State modifiedState = updateStateWithBackOff(timeoutInNanos);
        // 当前线程是否需要阻塞等待
        boolean result = waitForPermissionIfNecessary(timeoutInNanos, modifiedState.nanosToWait);
        publishRateLimiterEvent(result);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long reservePermission(Duration timeoutDuration) {
        long timeoutInNanos = timeoutDuration.toNanos();
        State modifiedState = updateStateWithBackOff(timeoutInNanos);

        boolean canAcquireImmediately = modifiedState.nanosToWait <= 0;
        // 无需等待
        if (canAcquireImmediately) {
            publishRateLimiterEvent(true);
            return 0;
        }

        boolean canAcquireInTime = timeoutInNanos >= modifiedState.nanosToWait;
        // 返回等待的时间
        if (canAcquireInTime) {
            publishRateLimiterEvent(true);
            return modifiedState.nanosToWait;
        }

        publishRateLimiterEvent(false);
        return -1;
    }

    /**
     * Atomically updates the current {@link State} with the results of
     * applying the {@link AtomicRateLimiter#calculateNextState}, returning the updated {@link State}.
     * It differs from {@link AtomicReference#updateAndGet(UnaryOperator)} by constant back off.
     * It means that after one try to {@link AtomicReference#compareAndSet(Object, Object)}
     * this method will wait for a while before try one more time.
     * This technique was originally described in this
     * <a href="https://arxiv.org/abs/1305.5800"> paper</a>
     * and showed great results with {@link AtomicRateLimiter} in benchmark tests.
     *
     * @param timeoutInNanos a side-effect-free function
     * @return the updated value
     */
    private State updateStateWithBackOff(final long timeoutInNanos) {
        AtomicRateLimiter.State prev;
        AtomicRateLimiter.State next;
        // CAS
        do {
            prev = state.get();
            // 计算下一个状态
            next = calculateNextState(timeoutInNanos, prev);
        } while (!compareAndSet(prev, next));
        return next;
    }
    /** AtomicReference CAS **/
    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * It differs from {@link AtomicReference#updateAndGet(UnaryOperator)} by constant back off.
     * It means that after one try to {@link AtomicReference#compareAndSet(Object, Object)}
     * this method will wait for a while before try one more time.
     * This technique was originally described in this
     * <a href="https://arxiv.org/abs/1305.5800"> paper</a>
     * and showed great results with {@link AtomicRateLimiter} in benchmark tests.
     *
     * @param current the expected value
     * @param next    the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    private boolean compareAndSet(final State current, final State next) {
        if (state.compareAndSet(current, next)) {
            return true;
        }
        parkNanos(1); // back-off
        return false;
    }

    /**
     * 从当前状态计算下一个状态
     */
    /**
     * A side-effect-free function that can calculate next {@link State} from current.
     * It determines time duration that you should wait for permission and reserves it for you,
     * if you'll be able to wait long enough.
     *
     * @param timeoutInNanos max time that caller can wait for permission in nanoseconds
     * @param activeState    current state of {@link AtomicRateLimiter}
     * @return next {@link State}
     */
    private State calculateNextState(final long timeoutInNanos, final State activeState) {
        // 时间周期，默认500纳秒
        long cyclePeriodInNanos = activeState.config.getLimitRefreshPeriodInNanos();
        // 每个周期允许的token数，默认50个
        int permissionsPerCycle = activeState.config.getLimitForPeriod();
        // 从限流器启动到当前，经过的纳秒数
        long currentNanos = currentNanoTime();
        // 周期号，计算当前应该属于第几周期
        long currentCycle = currentNanos / cyclePeriodInNanos;
        // 将当前状态的属性暂时赋值给下一个状态
        long nextCycle = activeState.activeCycle;
        // 有可能为负值
        int nextPermissions = activeState.activePermissions;
        // 判断下一个状态的周期是否与计算的实际周期相同，不同则重新计算周期号及允许的token数
        if (nextCycle != currentCycle) {
            long elapsedCycles = currentCycle - nextCycle;
            long accumulatedPermissions = elapsedCycles * permissionsPerCycle;
            nextCycle = currentCycle;
            // 随着时间时间流逝，每一个周期都会往桶里面新增令牌
            nextPermissions = (int) min(nextPermissions + accumulatedPermissions, permissionsPerCycle);
        }
        // 计算获取下一个token所需等待的时间
        long nextNanosToWait = nanosToWaitForPermission(
                cyclePeriodInNanos, permissionsPerCycle, nextPermissions, currentNanos, currentCycle
        );
        // 生成新的状态
        State nextState = reservePermissions(activeState.config, timeoutInNanos, nextCycle, nextPermissions, nextNanosToWait);
        return nextState;
    }

    /**
     * Calculates time to wait for next permission as
     * [time to the next cycle] + [duration of full cycles until reserved permissions expire]
     * 到下一个周期还需要多长时间 + 之前有多少人在排队
     * @param cyclePeriodInNanos   current configuration values
     * @param permissionsPerCycle  current configuration values
     * @param availablePermissions currently available permissions, can be negative if some permissions have been reserved
     * @param currentNanos         current time in nanoseconds
     * @param currentCycle         current {@link AtomicRateLimiter} cycle    @return nanoseconds to wait for the next permission
     */
    private long nanosToWaitForPermission(final long cyclePeriodInNanos, final int permissionsPerCycle,
                                          final int availablePermissions, final long currentNanos,
                                          final long currentCycle) {
        // 可用token大于零，则不用等待
        if (availablePermissions > 0) {
            return 0L;
        }
        // 计算下一个token所需等待的时间
        long nextCycleTimeInNanos = (currentCycle + 1) * cyclePeriodInNanos;
        long nanosToNextCycle = nextCycleTimeInNanos - currentNanos;
        int fullCyclesToWait = (-availablePermissions) / permissionsPerCycle;
        return (fullCyclesToWait * cyclePeriodInNanos) + nanosToNextCycle;
    }

    /**
     * Determines whether caller can acquire permission before timeout or not and then creates corresponding {@link State}.
     * Reserves permissions only if caller can successfully wait for permission.
     *
     * @param config
     * @param timeoutInNanos max time that caller can wait for permission in nanoseconds
     * @param cycle          cycle for new {@link State}
     * @param permissions    permissions for new {@link State}
     * @param nanosToWait    nanoseconds to wait for the next permission
     * @return new {@link State} with possibly reserved permissions and time to wait
     */
    private State reservePermissions(final RateLimiterConfig config, final long timeoutInNanos,
                                     final long cycle, final int permissions, final long nanosToWait) {
        boolean canAcquireInTime = timeoutInNanos >= nanosToWait;
        int permissionsWithReservation = permissions;
        // 月月说我们最多等10分钟，服务员说下一位还要等30分钟，不吃了！
        if (canAcquireInTime) {
            permissionsWithReservation--;
        }
        return new State(config, cycle, permissionsWithReservation, nanosToWait);
    }

    /**
     * If nanosToWait is bigger than 0 it tries to park {@link Thread} for nanosToWait but not longer then timeoutInNanos.
     *
     * @param timeoutInNanos max time that caller can wait
     * @param nanosToWait    nanoseconds caller need to wait
     * @return true if caller was able to wait for nanosToWait without {@link Thread#interrupt} and not exceed timeout
     */
    private boolean waitForPermissionIfNecessary(final long timeoutInNanos, final long nanosToWait) {
        boolean canAcquireImmediately = nanosToWait <= 0;
        boolean canAcquireInTime = timeoutInNanos >= nanosToWait;
        // 如果无需等待，则返回true，表示拿到token
        if (canAcquireImmediately) {
            return true;
        }
        if (canAcquireInTime) {
            return waitForPermission(nanosToWait);
        }
        waitForPermission(timeoutInNanos);
        return false;
    }
    /** 等待获取token **/
    /**
     * Parks {@link Thread} for nanosToWait.
     * <p>If the current thread is {@linkplain Thread#interrupted}
     * while waiting for a permit then it won't throw {@linkplain InterruptedException},
     * but its interrupt status will be set.
     *
     * @param nanosToWait nanoseconds caller need to wait
     * @return true if caller was not {@link Thread#interrupted} while waiting
     */
    private boolean waitForPermission(final long nanosToWait) {
        // 等待线程数加一
        waitingThreads.incrementAndGet();
        long deadline = currentNanoTime() + nanosToWait;
        boolean wasInterrupted = false;
        // 阻塞到下一个周期，或者当前线程被中断
        while (currentNanoTime() < deadline && !wasInterrupted) {
            long sleepBlockDuration = deadline - currentNanoTime();
            parkNanos(sleepBlockDuration);
            wasInterrupted = Thread.interrupted();
        }
        waitingThreads.decrementAndGet();
        if (wasInterrupted) {
            currentThread().interrupt();
        }
        return !wasInterrupted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RateLimiterConfig getRateLimiterConfig() {
        return state.get().config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metrics getMetrics() {
        return new AtomicRateLimiterMetrics();
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventProcessor;
    }

    @Override
    public String toString() {
        return "AtomicRateLimiter{" +
                "name='" + name + '\'' +
                ", rateLimiterConfig=" + state.get().config +
                '}';
    }

    /**
     * Get the enhanced Metrics with some implementation specific details.
     *
     * @return the detailed metrics
     */
    public AtomicRateLimiterMetrics getDetailedMetrics() {
        return new AtomicRateLimiterMetrics();
    }

    private void publishRateLimiterEvent(boolean permissionAcquired) {
        if (!eventProcessor.hasConsumers()) {
            return;
        }
        if (permissionAcquired) {
            eventProcessor.consumeEvent(new RateLimiterOnSuccessEvent(name));
            return;
        }
        eventProcessor.consumeEvent(new RateLimiterOnFailureEvent(name));
    }

    /** 限流器状态类，属性都是final的，不可变**/
    /**
     * <p>{@link AtomicRateLimiter.State} represents immutable state of {@link AtomicRateLimiter} where:
     * <ul>
     * <li>activeCycle - {@link AtomicRateLimiter} cycle number that was used
     * by the last {@link AtomicRateLimiter#getPermission(Duration)} call.</li>
     * <p>
     * <li>activePermissions - count of available permissions after
     * the last {@link AtomicRateLimiter#getPermission(Duration)} call.
     * Can be negative if some permissions where reserved.</li>
     * <p>
     * <li>nanosToWait - count of nanoseconds to wait for permission for
     * the last {@link AtomicRateLimiter#getPermission(Duration)} call.</li>
     * </ul>
     */
    private static class State {
        // 限流器配置
        private final RateLimiterConfig config;
        // 当前的周期号
        private final long activeCycle;
        // 当前周期下的token数：[只要timeoutInNanos >= nanosToWait就一直减,activeState.config.getLimitForPeriod()]
        private final int activePermissions;
        // 没有可用的token时，获取下一个周期的token，需等待的时间(纳秒)
        private final long nanosToWait;

        private State(RateLimiterConfig config,
                      final long activeCycle, final int activePermissions, final long nanosToWait) {
            this.config = config;
            this.activeCycle = activeCycle;
            this.activePermissions = activePermissions;
            this.nanosToWait = nanosToWait;
        }
    }

    /**
     * Enhanced {@link Metrics} with some implementation specific details
     */
    public class AtomicRateLimiterMetrics implements Metrics {

        private AtomicRateLimiterMetrics() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumberOfWaitingThreads() {
            return waitingThreads.get();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getAvailablePermissions() {
            State currentState = state.get();
            State estimatedState = calculateNextState(-1, currentState);
            return estimatedState.activePermissions;
        }

        /**
         * @return estimated time duration in nanos to wait for the next permission
         */
        public long getNanosToWait() {
            State currentState = state.get();
            State estimatedState = calculateNextState(-1, currentState);
            return estimatedState.nanosToWait;
        }

        /**
         * @return estimated current cycle
         */
        public long getCycle() {
            State currentState = state.get();
            State estimatedState = calculateNextState(-1, currentState);
            return estimatedState.activeCycle;
        }

    }
}
