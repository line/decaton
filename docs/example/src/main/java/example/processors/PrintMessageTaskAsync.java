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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.linecorp.decaton.example.protocol.Mytasks.PrintMessageTask;
import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;

public class PrintMessageTaskAsync implements DecatonProcessor<PrintMessageTask> {
    Producer<String, String> producer;

    @Override
    public void process(ProcessingContext<PrintMessageTask> context, PrintMessageTask task) throws InterruptedException {
        Completion completion = context.deferCompletion();
        producer.send(new ProducerRecord<>("next-topic", "Hello" + task.getName()),
                      (metadata, exception) -> completion.complete());
    }
}
