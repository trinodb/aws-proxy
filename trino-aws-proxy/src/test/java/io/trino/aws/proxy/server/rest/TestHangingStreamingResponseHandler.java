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
package io.trino.aws.proxy.server.rest;

import com.google.common.collect.ImmutableList;
import com.google.inject.BindingAnnotation;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.units.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.EOFException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHangingStreamingResponseHandler
{
    private static final Duration TIMEOUT = new Duration(1, TimeUnit.SECONDS);
    private static final Duration NO_TIMEOUT = new Duration(1, TimeUnit.DAYS);

    private final Injector injector;

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForTimeout {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForNoTimeout {}

    public TestHangingStreamingResponseHandler()
    {
        Module module = binder -> {
            jaxrsBinder(binder).bind(HangingResource.class);

            httpClientBinder(binder).bindHttpClient("test", ForTimeout.class)
                    .withConfigDefaults(config -> config.setRequestTimeout(TIMEOUT)
                            .setIdleTimeout(TIMEOUT));

            httpClientBinder(binder).bindHttpClient("test", ForNoTimeout.class)
                    .withConfigDefaults(config -> config.setRequestTimeout(NO_TIMEOUT)
                            .setIdleTimeout(NO_TIMEOUT));
        };

        List<Module> modules = ImmutableList.of(
                module,
                new TestingNodeModule(),
                new EventModule(),
                new JaxrsModule(),
                new JsonModule(),
                new TestingHttpServerModule());

        Bootstrap app = new Bootstrap(modules);
        injector = app.initialize();
    }

    @AfterAll
    public void shutDown()
    {
        injector.getInstance(LifeCycleManager.class).stop();
    }

    @Test
    public void testStreamingResponseWithHang()
    {
        URI baseUrl = injector.getInstance(TestingHttpServer.class).getBaseUrl();
        HttpClient httpClient = injector.getInstance(Key.get(HttpClient.class, ForNoTimeout.class));

        Request request = prepareGet().setUri(baseUrl).build();
        assertThatThrownBy(() -> httpClient.execute(request, createJsonResponseHandler(jsonCodec(String.class))))
                .hasRootCauseInstanceOf(EOFException.class);
    }
}
