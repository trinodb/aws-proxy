/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.s3.proxy.server;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.s3.proxy.server.BackgroundTasks.ErrorMode.CANCEL_ON_EXCEPTION;
import static io.trino.s3.proxy.server.BackgroundTasks.backgroundTasksBinder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBackgroundTasks
{
    private final BackgroundTasks backgroundTasks;
    private final AtomicInteger goodRunCount = new AtomicInteger();

    public TestBackgroundTasks()
    {
        // test binding via injected task
        Module module = binder -> {
            backgroundTasksBinder(binder).task("good", Duration.ofMillis(1)).addBinding(TestBackgroundTasks.class, TestBackgroundTasks::goodTask).toInstance(this);
            backgroundTasksBinder(binder).task("bad", Duration.ofMillis(1), Duration.ofMillis(1), CANCEL_ON_EXCEPTION).addBinding(TestBackgroundTasks.class, TestBackgroundTasks::badTask).toInstance(this);
        };
        backgroundTasks = Guice.createInjector(module)
                .getInstance(BackgroundTasks.class);
    }

    @BeforeAll
    public void start()
    {
        backgroundTasks.start();
    }

    @AfterAll
    public void stop()
    {
        backgroundTasks.stop();
    }

    @Test
    public void testGoodBackgroundTask()
    {
        await().until(() -> goodRunCount.get() > 10);
    }

    @Test
    public void testBadBackgroundTask()
    {
        await().until(() -> backgroundTasks.getServiceFailure("bad").map(t -> t instanceof UncheckedExecutionException).orElse(false));
    }

    private void badTask()
    {
        assertThat(Thread.currentThread().getName()).endsWith("bad");
        throw new UncheckedExecutionException(new RuntimeException("bad task"));
    }

    private void goodTask()
    {
        assertThat(Thread.currentThread().getName()).endsWith("good");
        goodRunCount.incrementAndGet();
    }
}
