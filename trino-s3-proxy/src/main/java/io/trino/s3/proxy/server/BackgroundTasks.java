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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.airlift.log.Logger;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.trino.s3.proxy.server.BackgroundTasks.ErrorMode.CONTINUE_ON_EXCEPTION;
import static java.util.Objects.requireNonNull;

public class BackgroundTasks
{
    private static final Logger log = Logger.get(BackgroundTasks.class);

    private final ServiceManager serviceManager;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public interface Task
    {
        void run()
                throws RuntimeException;
    }

    public enum ErrorMode
    {
        CONTINUE_ON_EXCEPTION,
        CANCEL_ON_EXCEPTION,
    }

    public interface BackgroundTaskBinder
    {
        <T> LinkedBindingBuilder<T> addBinding(Class<T> clazz, Consumer<T> method);

        <T> LinkedBindingBuilder<T> addBinding(TypeLiteral<T> typeLiteral, Consumer<T> method);

        <T> LinkedBindingBuilder<T> addBinding(Key<T> key, Consumer<T> method);
    }

    public interface BackgroundTasksBinder
    {
        LinkedBindingBuilder<BackgroundTask> addBinding();

        BackgroundTaskBinder task(String name, Duration initialDelay, Duration delay, ErrorMode errorMode);

        default BackgroundTaskBinder task(String name, Duration delay)
        {
            return task(name, delay, delay, CONTINUE_ON_EXCEPTION);
        }
    }

    public static BackgroundTasksBinder backgroundTasksBinder(Binder binder)
    {
        Multibinder<BackgroundTask> multibinder = newSetBinder(binder, BackgroundTask.class);

        return new BackgroundTasksBinder()
        {
            @Override
            public LinkedBindingBuilder<BackgroundTask> addBinding()
            {
                return multibinder.addBinding();
            }

            @Override
            public BackgroundTaskBinder task(String name, Duration initialDelay, Duration delay, ErrorMode errorMode)
            {
                return new BackgroundTaskBinder()
                {
                    @Override
                    public <T> LinkedBindingBuilder<T> addBinding(Class<T> clazz, Consumer<T> method)
                    {
                        Key<T> key = Key.get(clazz, Names.named(name));
                        return addBinding(key, method);
                    }

                    @Override
                    public <T> LinkedBindingBuilder<T> addBinding(TypeLiteral<T> typeLiteral, Consumer<T> method)
                    {
                        Key<T> key = Key.get(typeLiteral, Names.named(name));
                        return addBinding(key, method);
                    }

                    @Override
                    public <T> LinkedBindingBuilder<T> addBinding(Key<T> key, Consumer<T> method)
                    {
                        Provider<BackgroundTask> provider = buildProvider(key, method, name, initialDelay, delay, errorMode);
                        multibinder.addBinding().toProvider(provider).in(Scopes.SINGLETON);
                        return binder.bind(key);
                    }
                };
            }

            private <T> Provider<BackgroundTask> buildProvider(Key<T> key, Consumer<T> method, String name, Duration initialDelay, Duration delay, ErrorMode errorMode)
            {
                return new Provider<>()
                {
                    @Inject private Injector injector;

                    @Override
                    public BackgroundTask get()
                    {
                        T instance = injector.getInstance(key);
                        Task task = () -> method.accept(instance);
                        return new BackgroundTask(name, task, initialDelay, delay, errorMode);
                    }
                };
            }
        };
    }

    public record BackgroundTask(String name, Task task, Duration initialDelay, Duration delay, ErrorMode errorMode)
    {
        public BackgroundTask
        {
            requireNonNull(name, "name is null");
            requireNonNull(task, "task is null");
            requireNonNull(initialDelay, "initialDelay is null");
            requireNonNull(delay, "delay is null");
            requireNonNull(errorMode, "errorMode is null");
        }

        public BackgroundTask(String name, Task task, Duration delay)
        {
            this(name, task, delay, delay, CONTINUE_ON_EXCEPTION);
        }
    }

    @Inject
    public BackgroundTasks(Set<BackgroundTask> backgroundTasks)
    {
        serviceManager = new ServiceManager(buildServices(executor, backgroundTasks));
    }

    public ExecutorService executor()
    {
        return executor;
    }

    @PostConstruct
    public void start()
    {
        serviceManager.startAsync();
    }

    @PreDestroy
    public void stop()
    {
        // TODO add logging - check for false result

        try {
            serviceManager.stopAsync().awaitStopped(Duration.ofSeconds(30));
        }
        catch (TimeoutException e) {
            // TODO
        }
        if (!shutdownAndAwaitTermination(executor, Duration.ofSeconds(30))) {
            // TODO
        }
    }

    @SuppressWarnings("SameParameterValue")
    @VisibleForTesting
    Optional<Throwable> getServiceFailure(String serviceName)
    {
        Service service = serviceManager.servicesByState()
                .values()
                .stream()
                .filter(thisService -> (thisService instanceof InternalService internalService) && internalService.taskDetails().name.equals(serviceName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Service " + serviceName + " not found"));

        return (service.state() == Service.State.FAILED) ? Optional.of(service.failureCause()) : Optional.empty();
    }

    private abstract static class InternalService
            extends AbstractService
    {
        protected abstract BackgroundTask taskDetails();
    }

    private static Set<AbstractService> buildServices(ExecutorService executor, Set<BackgroundTask> backgroundTasks)
    {
        return backgroundTasks.stream()
                .map(backgroundTask -> new InternalService()
                {
                    private volatile Future<Object> future;
                    private volatile boolean isDone;

                    @Override
                    protected BackgroundTask taskDetails()
                    {
                        return backgroundTask;
                    }

                    @Override
                    protected void doStart()
                    {
                        future = executor.submit(() -> {
                            Thread.currentThread().setName("background-thread-" + backgroundTask.name);
                            log.info("Starting background task. Name: %s", backgroundTask.name);

                            notifyStarted();

                            Duration nextDelay = backgroundTask.initialDelay;

                            try {
                                outer: while (true) {
                                    if (isDone || Thread.currentThread().isInterrupted()) {
                                        notifyStopped();
                                        break;
                                    }

                                    try {
                                        Thread.sleep(nextDelay);
                                        nextDelay = backgroundTask.delay;

                                        backgroundTask.task.run();
                                    }
                                    catch (InterruptedException _) {
                                        log.info("Background task interrupted. Name: %s", backgroundTask.name);

                                        isDone = true;
                                    }
                                    catch (Throwable e) {
                                        log.error(e, "Background task error. Name: %s", backgroundTask.name);

                                        switch (backgroundTask.errorMode) {
                                            case CONTINUE_ON_EXCEPTION -> {}    // do nothing

                                            case CANCEL_ON_EXCEPTION -> {
                                                notifyFailed(e);
                                                break outer;
                                            }
                                        }
                                    }
                                }
                            }
                            finally {
                                isDone = true;
                            }

                            log.info("Background task exiting. Name: %s", backgroundTask.name);

                            return null;
                        });
                    }

                    @Override
                    protected void doStop()
                    {
                        if (future != null) {
                            future.cancel(true);
                        }
                        isDone = true;
                    }
                })
                .collect(toImmutableSet());
    }
}
