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
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;

/**
 * Abstract state of the CircuitBreaker state machine.
 */
abstract class CircuitBreakerState{

    // 有限状态机实例，内部实现了状态转换机制
    CircuitBreakerStateMachine stateMachine;

    CircuitBreakerState(CircuitBreakerStateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    /**
     * 是否允许请求调用后端接口
     * @return
     */
    abstract boolean isCallPermitted();

    /**
     * 请求调用失败，记录指标。
     * 当达到预设的度量指标值后，调用状态机实例触发状态转换
     * @param throwable
     */
    abstract void onError(Throwable throwable);

    /**
     * 请求调用成功，记录指标。
     * 当达到预设的度量指标值后，调用状态机实例触发状态转换
     */
    abstract void onSuccess();

    /**
     * 返回当前状态在CircuitBreaker接口中对应的枚举值
     * @return
     */
    abstract CircuitBreaker.State getState();

    /**
     * 返回当前状态的封装了度量指标的类实例
     * @return
     */
    abstract CircuitBreakerMetrics getMetrics();

    /**
     * 是否发布事件
     * Should the CircuitBreaker in this state publish events
     * @return a boolean signaling if the events should be published
     */
    boolean shouldPublishEvents(CircuitBreakerEvent event){
        return event.getEventType().forcePublish || getState().allowPublish;
    }
}
