package io.github.resilience4j.retry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.github.resilience4j.core.EventConsumer;
import io.github.resilience4j.retry.event.RetryEvent;
import io.github.resilience4j.retry.event.RetryOnErrorEvent;
import io.github.resilience4j.retry.event.RetryOnIgnoredErrorEvent;
import io.github.resilience4j.retry.event.RetryOnRetryEvent;
import io.github.resilience4j.retry.event.RetryOnSuccessEvent;
import io.github.resilience4j.retry.internal.AsyncRetryImpl;

/**
 * A AsyncRetry instance is thread-safe can be used to decorate multiple requests.
 */
public interface AsyncRetry {

    /**
     * Returns the ID of this Retry.
     *
     * @return the ID of this Retry
     */
    String getName();

    /**
     * Returns an EventPublisher which can be used to register event consumers.
     *
     * @return an EventPublisher
     */
    EventPublisher getEventPublisher();

    /**
     * Creates a retry Context.
     *
     * @return the retry Context
     */
    AsyncRetry.Context context();

    /**
     * Returns the RetryConfig of this Retry.
     *
     * @return the RetryConfig of this Retry
     */
    RetryConfig getRetryConfig();

    /**
     * Creates a Retry with a custom Retry configuration.
     *
     * @param id the ID of the Retry
     * @param retryConfig a custom Retry configuration
     *
     * @return a Retry with a custom Retry configuration.
     */
    static AsyncRetry of(String id, RetryConfig retryConfig){
        return new AsyncRetryImpl(id, retryConfig);
    }

    /**
     * Creates a Retry with a custom Retry configuration.
     *
     * @param id the ID of the Retry
     * @param retryConfigSupplier a supplier of a custom Retry configuration
     *
     * @return a Retry with a custom Retry configuration.
     */
    static AsyncRetry of(String id, Supplier<RetryConfig> retryConfigSupplier){
        return of(id, retryConfigSupplier.get());
    }

    /**
     * Creates a Retry with default configuration.
     *
     * @param id the ID of the Retry
     * @return a Retry with default configuration
     */
    static AsyncRetry ofDefaults(String id){
        return of(id, RetryConfig.ofDefaults());
    }

    /**
     * Decorates and executes the decorated CompletionStage.

     * @param scheduler execution service to use to schedule retries
     * @param supplier the original CompletionStage
     * @param <T> the type of results supplied by this supplier
     * @return the decorated CompletionStage.
     */
    default <T> CompletionStage<T> executeCompletionStage( ScheduledExecutorService scheduler, Supplier<CompletionStage<T>> supplier){
        return decorateCompletionStage(this, scheduler, supplier).get();
    }

    /**
     * Decorates CompletionStageSupplier with Retry
     *
     * @param retry the retry context
     * @param scheduler execution service to use to schedule retries
     * @param supplier completion stage supplier
     * @param <T> type of completion stage result
     * @return decorated supplier
     */
    static <T> Supplier<CompletionStage<T>> decorateCompletionStage(
        AsyncRetry retry,
        ScheduledExecutorService scheduler,
        Supplier<CompletionStage<T>> supplier
    ) {
        return () -> {

            final CompletableFuture<T> promise = new CompletableFuture<>();
            // 启动一个异步 schedule 线程进行重试
	        @SuppressWarnings("unchecked")
            final Runnable block = new AsyncRetryBlock<>(scheduler, retry.context(), supplier, promise);
            block.run();

            return promise;
        };
    }

    /**
     * Get the Metrics of this RateLimiter.
     *
     * @return the Metrics of this RateLimiter
     */
    Metrics getMetrics();

    interface Metrics {

        /**
         * Returns the number of successful calls without a retry attempt.
         *
         * @return the number of successful calls without a retry attempt
         */
        long getNumberOfSuccessfulCallsWithoutRetryAttempt();

        /**
         * Returns the number of failed calls without a retry attempt.
         *
         * @return the number of failed calls without a retry attempt
         */
        long getNumberOfFailedCallsWithoutRetryAttempt();

        /**
         * Returns the number of successful calls after a retry attempt.
         *
         * @return the number of successful calls after a retry attempt
         */
        long getNumberOfSuccessfulCallsWithRetryAttempt();

        /**
         * Returns the number of failed calls after all retry attempts.
         *
         * @return the number of failed calls after all retry attempts
         */
        long getNumberOfFailedCallsWithRetryAttempt();
    }

	interface Context<T> {
        /** 调用成功，记录相关监控数据 */
        /**
         *  Records a successful call.
         */
        void onSuccess();
        /** 记录调用失败 */
        /**
         * Records an failed call.
         * @param throwable the exception to handle
         * @return delay in milliseconds until the next try
         */
        long onError(Throwable throwable);
        /** 检查调用结果 */
		/**
		 * check the result call.
		 *
		 * @param result the  result to validate
		 * @return delay in milliseconds until the next try if the result match the predicate
		 */
		long onResult(T result);
    }

    /**
     * An EventPublisher which subscribes to the reactive stream of RetryEvents and
     * can be used to register event consumers.
     */
    interface EventPublisher extends io.github.resilience4j.core.EventPublisher<RetryEvent> {

        EventPublisher onRetry(EventConsumer<RetryOnRetryEvent> eventConsumer);

        EventPublisher onSuccess(EventConsumer<RetryOnSuccessEvent> eventConsumer);

        EventPublisher onError(EventConsumer<RetryOnErrorEvent> eventConsumer);

        EventPublisher onIgnoredError(EventConsumer<RetryOnIgnoredErrorEvent> eventConsumer);

    }
}

class AsyncRetryBlock<T> implements Runnable {
    private final ScheduledExecutorService scheduler;
	private final AsyncRetry.Context<T> retryContext;
    private final Supplier<CompletionStage<T>> supplier;
    private final CompletableFuture<T> promise;

    AsyncRetryBlock(
		    ScheduledExecutorService scheduler,
		    AsyncRetry.Context<T> retryContext,
		    Supplier<CompletionStage<T>> supplier,
		    CompletableFuture<T> promise
    ) {
        this.scheduler = scheduler;
        this.retryContext = retryContext;
        this.supplier = supplier;
        this.promise = promise;
    }

    @Override
    public void run() {
        final CompletionStage<T> stage;

        try {
            stage = supplier.get();
        } catch (Throwable t) {
            onError(t);
            return;
        }
        // 调用回调函数
        stage.whenComplete((result, t) -> {
	        if (result != null) {
	            // 有结果
		        onResult(result);
	        } else if (t != null) {
	            // 有异常
                onError(t);
            }
        });
    }

    private void onError(Throwable t) {
        final long delay = retryContext.onError(t);
        // 不符合异常预期
        if (delay < 1) {
            // 抛出异常
            promise.completeExceptionally(t);
        } else {
            // 每隔delay毫秒，执行一次本线程
            scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
        }
    }

	private void onResult(T result) {
		final long delay = retryContext.onResult(result);
        // 不符合重试调结果预期
		if (delay < 1) {
			promise.complete(result);
			retryContext.onSuccess();
		} else {
            // 每隔delay毫秒，执行一次本线程
			scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
		}
	}
}
