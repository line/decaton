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

import java.util.List;
import java.util.stream.Collectors;

import com.linecorp.decaton.processor.processors.BatchingProcessor;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class InsertHelloTask extends BatchingProcessor<HelloTask> {
    public InsertHelloTask(long lingerMillis, int capacity) {
        super(lingerMillis, capacity);
    }

    @Override
    protected void processBatchingTasks(List<BatchingTask<HelloTask>> batchingTasks) {
        List<HelloTask> helloTasks =
                batchingTasks.stream().map(BatchingTask::task).collect(Collectors.toList());
        helloTasks.forEach(task -> System.out.println("Processing task: " + task)); // (Process helloTasks)
        batchingTasks.forEach(batchingTask -> batchingTask.completion().complete());
    }
}
