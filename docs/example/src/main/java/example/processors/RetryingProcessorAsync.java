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

package example.processors;

import java.util.concurrent.CompletableFuture;

import com.linecorp.decaton.example.protocol.Mytasks.PrintMessageTask;
import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;

public class RetryingProcessorAsync implements DecatonProcessor<PrintMessageTask> {

    @Override
    public void process(ProcessingContext<PrintMessageTask> context, PrintMessageTask task)
            throws InterruptedException {

        Completion completion = context.deferCompletion();

        CompletableFuture<String> userInfo = getUserInfoAsync("Test");
        userInfo.whenComplete((user, exception) -> {
            if (exception == null) {
                // Do something...
                completion.complete();
            } else {
                try {
                    context.retry();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private CompletableFuture<String> getUserInfoAsync(String name) {
        return CompletableFuture.completedFuture("Test");
    }
}
