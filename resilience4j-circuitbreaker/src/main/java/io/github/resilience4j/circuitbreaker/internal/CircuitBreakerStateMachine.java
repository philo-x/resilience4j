/*
 *
 *  Copyright 2016 Robert Winkler
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
package io.github.resilience4j.circuitbreaker.internal;


import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.DISABLED;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.FORCED_OPEN;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.HALF_OPEN;
import static io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnCallNotPermittedEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnErrorEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnIgnoredErrorEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnResetEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnSuccessEvent;
import io.github.resilience4j.core.EventConsumer;
import io.github.resilience4j.core.EventProcessor;

/**
 * A CircuitBreaker finite state machine.
 */
public final class CircuitBreakerStateMachine implements CircuitBreaker {

    private static final Logger LOG = LoggerFactory.getLogger(CircuitBreakerStateMachine.class);

    private final String name;
    // 用AtomicReference保证对CircuitBreakerState引用的原子性
    private final AtomicReference<CircuitBreakerState> stateReference;
    // 熔断器配置
    private final CircuitBreakerConfig circuitBreakerConfig;
    // 事件处理器
    private final CircuitBreakerEventProcessor eventProcessor;

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration.
     */
    public CircuitBreakerStateMachine(String name, CircuitBreakerConfig circuitBreakerConfig) {
        // 设置熔断器名称
        this.name = name;
        // 设置熔断器配置
        this.circuitBreakerConfig = circuitBreakerConfig;
        // 将状态机初始化为关闭状态
        this.stateReference = new AtomicReference<>(new ClosedState(this));
        // 初始化熔断事件处理器
        this.eventProcessor = new CircuitBreakerEventProcessor();
    }

    /**
     * Creates a circuitBreaker with default config.
     *
     * @param name the name of the CircuitBreaker
     */
    public CircuitBreakerStateMachine(String name) {
        this(name, CircuitBreakerConfig.ofDefaults());
    }

    /**
     * Creates a circuitBreaker.
     *
     * @param name                 the name of the CircuitBreaker
     * @param circuitBreakerConfig The CircuitBreaker configuration supplier.
     */
    public CircuitBreakerStateMachine(String name, Supplier<CircuitBreakerConfig> circuitBreakerConfig) {
        this(name, circuitBreakerConfig.get());
    }

    /**
     * Requests permission to call this backend.
     *
     * @return true, if the call is allowed.
     */
    @Override
    public boolean isCallPermitted() {
        // 当前的状态是否允许请求调用
        boolean callPermitted = stateReference.get().isCallPermitted();
        if (!callPermitted) {
            // 发布不允许请求调用的事件
            publishCallNotPermittedEvent();
        }
        return callPermitted;
    }

    @Override
    public void onError(long durationInNanos, Throwable throwable) {
        // 判断请求调用抛出的异常是否需要记录，默认所有异常都需要记录
        if (circuitBreakerConfig.getRecordFailurePredicate().test(throwable)) {
            LOG.debug("CircuitBreaker '{}' recorded a failure:", name, throwable);
            // 发布请求调用失败事件
            publishCircuitErrorEvent(name, durationInNanos, throwable);
            // 调用当前状态的onError(throwable)方法进行失败率统计，当达到阈值时触发状态转换
            stateReference.get().onError(throwable);
        } else {
            // 发布忽略异常事件
            publishCircuitIgnoredErrorEvent(name, durationInNanos, throwable);
        }
    }

    @Override
    public void onSuccess(long durationInNanos) {
        // 发布请求调用成功事件
        publishSuccessEvent(durationInNanos);
        // 调用当前状态的onSuccess()方法进行失败率统计，当达到阈值时触发状态转换
        stateReference.get().onSuccess();
    }



    /**
     * Get the state of this CircuitBreaker.
     *
     * @return the the state of this CircuitBreaker
     */
    @Override
    public State getState() {
        return this.stateReference.get().getState();
    }

    /**
     * Get the name of this CircuitBreaker.
     *
     * @return the the name of this CircuitBreaker
     */
    @Override
    public String getName() {
        return this.name;
    }


    /**
     * Get the config of this CircuitBreaker.
     *
     * @return the config of this CircuitBreaker
     */
    @Override
    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return circuitBreakerConfig;
    }

    @Override
    public Metrics getMetrics() {
        return this.stateReference.get().getMetrics();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("CircuitBreaker '%s'", this.name);
    }

    @Override
    public void reset() {
        CircuitBreakerState previousState = stateReference.getAndUpdate(currentState -> new ClosedState(this));
        if (previousState.getState() != CLOSED) {
            publishStateTransitionEvent(StateTransition.transitionBetween(previousState.getState(), CLOSED));
        }
        publishResetEvent();
    }

    /**
     * 状态转换公共逻辑方法
     * @param newState
     * @param newStateGenerator
     */
    private void stateTransition(State newState, Function<CircuitBreakerState, CircuitBreakerState> newStateGenerator) {
        // 先获取当前状态，然后执行状态更新方法。由AtomicReference保证操作的原子性。
        CircuitBreakerState previousState = stateReference.getAndUpdate(currentState -> {
            // 新状态如果与当前状态相等，则返回当前状态。采用枚举判等
            if (currentState.getState() == newState) {
                return currentState;
            }
            // 执行更新方法，stateReference持有新状态的引用
            return newStateGenerator.apply(currentState);
        });
        // 如果当前状态与新状态不等，则发布状态转换事件
        if (previousState.getState() != newState) {
            publishStateTransitionEvent(StateTransition.transitionBetween(previousState.getState(), newState));
        }
    }

    /**
     * 转换到不可用状态
     */
    @Override
    public void transitionToDisabledState() {
        stateTransition(DISABLED, currentState -> new DisabledState(this));
    }

    /**
     * 转换到强制打开状态
     */
    @Override
    public void transitionToForcedOpenState() {
        stateTransition(FORCED_OPEN, currentState -> new ForcedOpenState(this));
    }

    /**
     * 转换到关闭状态
     */
    @Override
    public void transitionToClosedState() {
        stateTransition(CLOSED, currentState -> new ClosedState(this, currentState.getMetrics()));
    }

    /**
     * 转换到打开状态
     */
    @Override
    public void transitionToOpenState() {
        stateTransition(OPEN, currentState -> new OpenState(this, currentState.getMetrics()));
    }

    /**
     * 转换到半开状态
     */
    @Override
    public void transitionToHalfOpenState() {
        stateTransition(HALF_OPEN, currentState -> new HalfOpenState(this));
    }


    private boolean shouldPublishEvents(CircuitBreakerEvent event) {
        return stateReference.get().shouldPublishEvents(event);
    }


    private void publishEventIfPossible(CircuitBreakerEvent event) {
        if(shouldPublishEvents(event)) {
            if (eventProcessor.hasConsumers()) {
                LOG.debug("Event {} published: {}", event.getEventType(), event);
                try{
                    eventProcessor.consumeEvent(event);
                }catch (Throwable t){
                    LOG.warn("Failed to handle event {}", event.getEventType(), t);
                }
            } else {
                LOG.debug("No Consumers: Event {} not published", event.getEventType());
            }
        } else {
            LOG.debug("Publishing not allowed: Event {} not published", event.getEventType());
        }
    }

    private void publishStateTransitionEvent(final StateTransition stateTransition) {
        final CircuitBreakerOnStateTransitionEvent event = new CircuitBreakerOnStateTransitionEvent(name, stateTransition);
        publishEventIfPossible(event);
    }

    private void publishResetEvent() {
        final CircuitBreakerOnResetEvent event = new CircuitBreakerOnResetEvent(name);
        publishEventIfPossible(event);
    }

    private void publishCallNotPermittedEvent() {
        final CircuitBreakerOnCallNotPermittedEvent event = new CircuitBreakerOnCallNotPermittedEvent(name);
        publishEventIfPossible(event);
    }

    private void publishSuccessEvent(final long durationInNanos) {
        final CircuitBreakerOnSuccessEvent event = new CircuitBreakerOnSuccessEvent(name, Duration.ofNanos(durationInNanos));
        publishEventIfPossible(event);
    }

    private void publishCircuitErrorEvent(final String name, final long durationInNanos, final Throwable throwable) {
        final CircuitBreakerOnErrorEvent event = new CircuitBreakerOnErrorEvent(name, Duration.ofNanos(durationInNanos), throwable);
        publishEventIfPossible(event);
    }

    private void publishCircuitIgnoredErrorEvent(String name, long durationInNanos, Throwable throwable) {
        final CircuitBreakerOnIgnoredErrorEvent event = new CircuitBreakerOnIgnoredErrorEvent(name, Duration.ofNanos(durationInNanos), throwable);
        publishEventIfPossible(event);
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventProcessor;
    }

    private class CircuitBreakerEventProcessor extends EventProcessor<CircuitBreakerEvent> implements EventConsumer<CircuitBreakerEvent>, EventPublisher {
        @Override
        public EventPublisher onSuccess(EventConsumer<CircuitBreakerOnSuccessEvent> onSuccessEventConsumer) {
            // 注册用于处理请求调用成功事件的consumer
            registerConsumer(CircuitBreakerOnSuccessEvent.class, onSuccessEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onError(EventConsumer<CircuitBreakerOnErrorEvent> onErrorEventConsumer) {
            registerConsumer(CircuitBreakerOnErrorEvent.class, onErrorEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onStateTransition(EventConsumer<CircuitBreakerOnStateTransitionEvent> onStateTransitionEventConsumer) {
            registerConsumer(CircuitBreakerOnStateTransitionEvent.class, onStateTransitionEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onReset(EventConsumer<CircuitBreakerOnResetEvent> onResetEventConsumer) {
            registerConsumer(CircuitBreakerOnResetEvent.class, onResetEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onIgnoredError(EventConsumer<CircuitBreakerOnIgnoredErrorEvent> onIgnoredErrorEventConsumer) {
            registerConsumer(CircuitBreakerOnIgnoredErrorEvent.class, onIgnoredErrorEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onCallNotPermitted(EventConsumer<CircuitBreakerOnCallNotPermittedEvent> onCallNotPermittedEventConsumer) {
            registerConsumer(CircuitBreakerOnCallNotPermittedEvent.class, onCallNotPermittedEventConsumer);
            return this;
        }

        /** 调用父类EventProcessor.processEvent方法处理事件 **/
        @Override
        public void consumeEvent(CircuitBreakerEvent event) {
            super.processEvent(event);
        }
    }
}
