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

package com.linecorp.decaton.processor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

import com.linecorp.decaton.processor.Completion.TimeoutChoice;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;

public interface ProcessingContext<T> {
    /**
     * Returns a {@link TaskMetadata} which is associated with the task that is currently being processed.
     * @return an instance of {@link TaskMetadata}
     */
    TaskMetadata metadata();

    /**
     * Returns the key which is associated to the task now being processed.
     *
     * @return the key associated to the task now being processed. can be null if key isn't supplied for the
     * task.
     */
    byte[] key();

    /**
     * Returns the {@link Headers} which is associated to the source {@link ConsumerRecord} of the task.
     * @return an instance of {@link Headers}
     */
    Headers headers();

    /**
     * Returns the subscriptionId of the current processing context.
     *
     * @return the subscription ID configured for this processing context.
     */
    String subscriptionId();

    /**
     * Creates a {@link LoggingContext} and populate it with implementation specific logging context
     * @return logging context
     */
    LoggingContext loggingContext();

    /**
     * Tells the completion of this processing should be postponed and processor can accept next task.
     * Once this method called within {@link DecatonProcessor#process} method, caller *MUST* call
     * {@link DeferredCompletion#complete()} or {@link ProcessingContext#retry()} method in any cases.
     * Otherwise consumption will stuck in short future and no new task will be given to the processor.
     * To avoid that with some risks, consider setting
     * {@link ProcessorProperties#CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS}.
     * @return a {@link Completion} which can be used to tell the completion of processing asynchronously.
     */
    default Completion deferCompletion() {
        return deferCompletion(comp -> TimeoutChoice.GIVE_UP);
    }

    /**
     * Tells the completion of this processing should be postponed and processor can accept next task.
     * Once this method called within {@link DecatonProcessor#process} method, caller *MUST* call
     * {@link DeferredCompletion#complete()} or {@link ProcessingContext#retry()} method in any cases.
     * Otherwise consumption will stuck in short future and no new task will be given to the processor.
     * To avoid that with some risks, consider setting
     * {@link ProcessorProperties#CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS}.
     *
     * This method takes a callback that is called when the returned completion "timed out".
     * For the detail of deferred completion timeout, see
     * {@link ProcessorProperties#CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS}.
     * You can do several things with the callback.
     *
     * If you know that the asynchronous processing is taking longer than expected but still running normally,
     * thus want to tell decaton to extend timeout and let the processing complete, you can simply return
     * {@link TimeoutChoice#EXTEND} from the callback.
     * {@code
     * context.deferCompletion(comp -> {
     *     if (checkEverythingOk()) {
     *         return TimeoutChoice.EXTEND;
     *     }
     *     return TimeoutChoice.GIVE_UP;
     * });
     * }

     * If you've configured "retry" feature (see {@link SubscriptionBuilder#enableRetry(RetryConfig)}), you can
     * send the timed out task to retry queue by calling {@link #retry()}.
     * {@code
     * context.deferCompletion(comp -> {
     *     comp.completeWith(context.retry()); // Rebind completion to the completion of retry queueing
     *     return TimeoutChoice.EXTEND;
     * });
     * }
     *
     * Note that you must return {@link TimeoutChoice#EXTEND} from the callback even in this case to make sure
     * that decaton waits until it completes retry-queuing asynchronously.
     *
     * if the callback returns false, decaton times out completion and forcefully completes it.
     *
     * @param callback callback which is called when the returned completion times out.
     * @return a {@link Completion} which can be used to tell the completion of processing asynchronously.
     */
    Completion deferCompletion(Function<Completion, TimeoutChoice> callback);

    /**
     * Sends given task to downstream processors if exists.
     * Calling this method lets downstream processor to process a task immediately, but if the downstream
     * processor defers process's completion by calling {@link ProcessingContext#deferCompletion()}, the
     * returned {@link CompletableFuture} completes asynchronously.
     * If the completion of the current task depends on downstream's processing, the current processing
     * also needs to defer its completion along with binding completion to downstream's future like:
     * {@code context.deferCompletion().completeWith(context.push(task));}
     * If the current processing needs to call {@link #push} multiple times and if it depends on
     * all results, {@link CompletableFuture#allOf(CompletableFuture[])} can be utilized to gather all results
     * into one {@link CompletableFuture}:
     * {@code context.deferCompletion().completeWith(CompletableFuture.allOf(result1, result2);}
     *
     * @param task a task of type {@link T} to send to downstream
     * @return a {@link CompletableFuture} that completes when downstream processor completes processing.
     * @throws InterruptedException when processing gets interrupted.
     */
    Completion push(T task) throws InterruptedException;

    /**
     * Schedule the currently processing task for retrying.
     * The task currently being processed is queued for future retrying, and processed again in the future
     * *at least after* configured backoff elapsed.
     * If {@link ProcessingContext#deferCompletion()} was called before, {@link ProcessingContext#retry()}
     * will automatically complete deferred processing after the currect task is queued for retrying.
     * @return a {@link CompletableFuture} that completes on the producer completed or failed to produce
     * retry task.
     * @throws InterruptedException when processing gets interrupted.
     */
    Completion retry() throws InterruptedException;
}
