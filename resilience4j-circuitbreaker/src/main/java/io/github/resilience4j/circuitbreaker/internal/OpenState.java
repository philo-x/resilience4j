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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import java.time.Duration;
import java.time.Instant;

final class OpenState extends CircuitBreakerState {
    // 打开状态的持续时间，
    private final Instant retryAfterWaitDuration;
    // 打开状态的度量指标，在配置类CircuitBreakerConfig的实例中已设置
    private final CircuitBreakerMetrics circuitBreakerMetrics;

    OpenState(CircuitBreakerStateMachine stateMachine, CircuitBreakerMetrics circuitBreakerMetrics) {
        super(stateMachine);
        final Duration waitDurationInOpenState = stateMachine.getCircuitBreakerConfig().getWaitDurationInOpenState();
        this.retryAfterWaitDuration = Instant.now().plus(waitDurationInOpenState);
        this.circuitBreakerMetrics = circuitBreakerMetrics;

        if (stateMachine.getCircuitBreakerConfig().isAutomaticTransitionFromOpenToHalfOpenEnabled()) {
            AutoTransitioner.scheduleAutoTransition(stateMachine::transitionToHalfOpenState, waitDurationInOpenState);
        }
    }

    /**
     * 如果到达了打开状态的持续时间，则触发状态机，从打开状态转换到半开状态，允许请求调用后端接口
     * 否则返回false，不允许请求调用后端接口
     */
    @Override
    boolean isCallPermitted() {
        // Thread-safe
        if (Instant.now().isAfter(retryAfterWaitDuration)) {
            // 从打开状态转换到半开状态
            stateMachine.transitionToHalfOpenState();
            return true;
        }
        circuitBreakerMetrics.onCallNotPermitted();
        return false;
    }

    /**
     * Should never be called when isCallPermitted returns false.
     */
    @Override
    void onError(Throwable throwable) {
        // Could be called when Thread 1 invokes isCallPermitted when the state is CLOSED, but in the meantime another
        // Thread 2 calls onError and the state changes from CLOSED to OPEN before Thread 1 calls onError.
        // But the onError event should still be recorded, even if it happened after the state transition.
        circuitBreakerMetrics.onError();
    }

    /**
     * Should never be called when isCallPermitted returns false.
     */
    @Override
    void onSuccess() {
        // Could be called when Thread 1 invokes isCallPermitted when the state is CLOSED, but in the meantime another
        // Thread 2 calls onError and the state changes from CLOSED to OPEN before Thread 1 calls onSuccess.
        // But the onSuccess event should still be recorded, even if it happened after the state transition.
        circuitBreakerMetrics.onSuccess();
    }

    /**
     * Get the state of the CircuitBreaker
     */
    @Override
    CircuitBreaker.State getState() {
        return CircuitBreaker.State.OPEN;
    }

    @Override
    CircuitBreakerMetrics getMetrics() {
        return circuitBreakerMetrics;
    }
}
