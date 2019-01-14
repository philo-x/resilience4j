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
package io.github.resilience4j.retry.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.github.resilience4j.core.EventConsumer;
import io.github.resilience4j.core.EventProcessor;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.event.RetryEvent;
import io.github.resilience4j.retry.event.RetryOnErrorEvent;
import io.github.resilience4j.retry.event.RetryOnIgnoredErrorEvent;
import io.github.resilience4j.retry.event.RetryOnRetryEvent;
import io.github.resilience4j.retry.event.RetryOnSuccessEvent;
import io.vavr.CheckedConsumer;
import io.vavr.control.Option;
import io.vavr.control.Try;

public class RetryImpl<T> implements Retry {


    private final Metrics metrics;
    private final RetryEventProcessor eventProcessor;
    private final Predicate<T> resultPredicate;
    private String name;
    private RetryConfig config;
    private int maxAttempts;
    private Function<Integer, Long> intervalFunction;
    private Predicate<Throwable> exceptionPredicate;
    private LongAdder succeededAfterRetryCounter;
    private LongAdder failedAfterRetryCounter;
    private LongAdder succeededWithoutRetryCounter;
    private LongAdder failedWithoutRetryCounter;
    /*package*/ static CheckedConsumer<Long> sleepFunction = Thread::sleep;

    public RetryImpl(String name, RetryConfig config){
        this.name = name;
        this.config = config;
        this.maxAttempts = config.getMaxAttempts();
        this.intervalFunction = config.getIntervalFunction();
        this.exceptionPredicate = config.getExceptionPredicate();
        this.resultPredicate = config.getResultPredicate();
        this.metrics = this.new RetryMetrics();
        this.eventProcessor = new RetryEventProcessor();
        succeededAfterRetryCounter = new LongAdder();
        failedAfterRetryCounter = new LongAdder();
        succeededWithoutRetryCounter = new LongAdder();
        failedWithoutRetryCounter = new LongAdder();
    }

    public final class ContextImpl implements Retry.Context<T> {
        // 原子整型类，记录重试次数
        private final AtomicInteger numOfAttempts = new AtomicInteger(0);
        // 原子引用类，记录业务异常
        private final AtomicReference<Exception> lastException = new AtomicReference<>();
        // 原子引用类，记录运行时异常
        private final AtomicReference<RuntimeException> lastRuntimeException = new AtomicReference<>();

        private ContextImpl() {
        }
        /** 调用成功，记录相关监控数据 */
        public void onSuccess() {
            int currentNumOfAttempts = numOfAttempts.get();
            if(currentNumOfAttempts > 0){
                succeededAfterRetryCounter.increment();
                Throwable throwable = Option.of(lastException.get()).getOrElse(lastRuntimeException.get());
                // 发布重试成功事件，将重试次数和最后一次异常信息发布出去
                publishRetryEvent(() -> new RetryOnSuccessEvent(getName(), currentNumOfAttempts, throwable));
            }else{
                succeededWithoutRetryCounter.increment();
            }
        }
        public boolean onResult(T result) {
            // 判断是否符合调用结果预期
            if (null != resultPredicate && resultPredicate.test(result)) {
                int currentNumOfAttempts = numOfAttempts.incrementAndGet();
                // 重试次数大于等于最大重试次数则返回false，无需等待
                if (currentNumOfAttempts >= maxAttempts) {
                    return false;
                } else {
                    // 阻塞一段时间后，返回true，进行重试
	                waitIntervalAfterFailure(currentNumOfAttempts, null);
                    return true;
                }
            }
            return false;
        }

        public void onError(Exception exception) throws Throwable{
            // 符合异常预期，记录异常
            if(exceptionPredicate.test(exception)){
                lastException.set(exception);
                // 小于最大重试次数则阻塞等待，否则抛出此异常
                throwOrSleepAfterException();
            }else{
                // 不符合异常预期，则发布事件，抛出此异常
                failedWithoutRetryCounter.increment();
                publishRetryEvent(() -> new RetryOnIgnoredErrorEvent(getName(), exception));
                throw exception;
            }
        }
        public void onRuntimeError(RuntimeException runtimeException){
            // 符合运行时异常预期，记录运行时异常
            if(exceptionPredicate.test(runtimeException)){
                lastRuntimeException.set(runtimeException);
                // 小于最大重试次数则阻塞等待，否则抛出此异常
                throwOrSleepAfterRuntimeException();
            }else{
                // 不符合运行时异常预期，则发布事件，抛出此异常
                failedWithoutRetryCounter.increment();
                publishRetryEvent(() -> new RetryOnIgnoredErrorEvent(getName(), runtimeException));
                throw runtimeException;
            }
        }

        private void throwOrSleepAfterException() throws Exception {
            int currentNumOfAttempts = numOfAttempts.incrementAndGet();
            Exception throwable = lastException.get();
            if (currentNumOfAttempts >= maxAttempts) {
                failedAfterRetryCounter.increment();
                publishRetryEvent(() -> new RetryOnErrorEvent(getName(), currentNumOfAttempts, throwable));
                throw throwable;
            } else {
                waitIntervalAfterFailure(currentNumOfAttempts, throwable);
            }
        }

        private void throwOrSleepAfterRuntimeException(){
            int currentNumOfAttempts = numOfAttempts.incrementAndGet();
            RuntimeException throwable = lastRuntimeException.get();
            if(currentNumOfAttempts >= maxAttempts){
                failedAfterRetryCounter.increment();
                publishRetryEvent(() -> new RetryOnErrorEvent(getName(), currentNumOfAttempts, throwable));
                throw throwable;
            }else{
                waitIntervalAfterFailure(currentNumOfAttempts, throwable);
            }
        }
        /** 等待重试 */
        private void waitIntervalAfterFailure(int currentNumOfAttempts, Throwable throwable) {
            // wait interval until the next attempt should start
            long interval = intervalFunction.apply(numOfAttempts.get());
            publishRetryEvent(()-> new RetryOnRetryEvent(getName(), currentNumOfAttempts, throwable, interval));
            // sleepFunction = Thread::sleep
            Try.run(() -> sleepFunction.accept(interval))
                    .getOrElseThrow(ex -> lastRuntimeException.get());
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Context context() {
        return new ContextImpl();
    }

    @Override
    public RetryConfig getRetryConfig() {
        return config;
    }


    private void publishRetryEvent(Supplier<RetryEvent> event) {
        if(eventProcessor.hasConsumers()) {
            eventProcessor.consumeEvent(event.get());
        }
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventProcessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metrics getMetrics() {
        return this.metrics;
    }

    public final class RetryMetrics implements Metrics {
        private RetryMetrics() {
        }

        @Override
        public long getNumberOfSuccessfulCallsWithoutRetryAttempt() {
            return succeededWithoutRetryCounter.longValue();
        }

        @Override
        public long getNumberOfFailedCallsWithoutRetryAttempt() {
            return failedWithoutRetryCounter.longValue();
        }

        @Override
        public long getNumberOfSuccessfulCallsWithRetryAttempt() {
            return succeededAfterRetryCounter.longValue();
        }

        @Override
        public long getNumberOfFailedCallsWithRetryAttempt() {
            return failedAfterRetryCounter.longValue();
        }
    }

    private class RetryEventProcessor extends EventProcessor<RetryEvent> implements EventConsumer<RetryEvent>, EventPublisher {

        @Override
        public void consumeEvent(RetryEvent event) {
            super.processEvent(event);
        }

        @Override
        public EventPublisher onRetry(EventConsumer<RetryOnRetryEvent> onRetryEventConsumer) {
            registerConsumer(RetryOnRetryEvent.class, onRetryEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onSuccess(EventConsumer<RetryOnSuccessEvent> onSuccessEventConsumer) {
            registerConsumer(RetryOnSuccessEvent.class, onSuccessEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onError(EventConsumer<RetryOnErrorEvent> onErrorEventConsumer) {
            registerConsumer(RetryOnErrorEvent.class, onErrorEventConsumer);
            return this;
        }

        @Override
        public EventPublisher onIgnoredError(EventConsumer<RetryOnIgnoredErrorEvent> onIgnoredErrorEventConsumer) {
            registerConsumer(RetryOnIgnoredErrorEvent.class, onIgnoredErrorEventConsumer);
            return this;
        }
    }
}
