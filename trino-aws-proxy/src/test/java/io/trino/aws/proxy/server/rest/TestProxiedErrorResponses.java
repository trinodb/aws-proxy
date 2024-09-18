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
import com.google.common.collect.ImmutableMap;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.aws.proxy.server.remote.PathStyleRemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingRemoteS3Facade;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer.Builder;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TrinoAwsProxyTest(filters = TestProxiedErrorResponses.Filter.class)
public class TestProxiedErrorResponses
{
    private final S3Client internalClient;

    /**
     * Status code taken from https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
     */
    private static final List<HttpStatus> STATUS_CODES = ImmutableList.of(
            HttpStatus.BAD_REQUEST,
            HttpStatus.FORBIDDEN,
            HttpStatus.NOT_FOUND,
            HttpStatus.METHOD_NOT_ALLOWED,
            HttpStatus.CONFLICT,
            HttpStatus.LENGTH_REQUIRED,
            HttpStatus.PRECONDITION_FAILED,
            HttpStatus.REQUEST_RANGE_NOT_SATISFIABLE,
            HttpStatus.INTERNAL_SERVER_ERROR,
            HttpStatus.NOT_IMPLEMENTED,
            HttpStatus.SERVICE_UNAVAILABLE);

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForErrorResponseTest {}

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public Builder filter(Builder builder)
        {
            TestingHttpServer httpErrorResponseServer;
            try {
                httpErrorResponseServer = createTestingHttpErrorResponseServer();
                httpErrorResponseServer.start();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to start http error response server", e);
            }
            return builder.addModule(binder -> binder.bind(Key.get(TestingHttpServer.class, ForErrorResponseTest.class)).toInstance(httpErrorResponseServer));
        }
    }

    @Inject
    public TestProxiedErrorResponses(S3Client internalClient, TestingRemoteS3Facade delegatingFacade, @ForErrorResponseTest TestingHttpServer httpErrorResponseServer)
    {
        this.internalClient = requireNonNull(internalClient, "internal client is null");
        delegatingFacade.setDelegate(new PathStyleRemoteS3Facade((_, _) -> httpErrorResponseServer.getBaseUrl().getHost(), false, Optional.of(httpErrorResponseServer.getBaseUrl().getPort())));
    }

    @Test
    public void test()
    {
        for (HttpStatus status : STATUS_CODES) {
            assertThrownAwsError(status);
        }
    }

    private void assertThrownAwsError(HttpStatus status)
    {
        assertThatExceptionOfType(S3Exception.class).isThrownBy(() -> getFileFromStorage(internalClient, "status", String.valueOf(status.code())))
                .satisfies(
                        exception -> assertThat(exception.statusCode()).isEqualTo(status.code()),
                        exception -> assertThat(exception.awsErrorDetails().errorCode()).isEqualTo(status.reason()));
    }

    private static TestingHttpServer createTestingHttpErrorResponseServer()
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpPort(0);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new HttpErrorResponseServlet(), ImmutableMap.of());
    }

    private static class HttpErrorResponseServlet
            extends HttpServlet
    {
        private static final String RESPONSE_TEMPLATE = """
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>%s</Code>
  <Message>Error Message</Message>
  <Resource>%s</Resource>
  <RequestId>123</RequestId>
</Error>""";

        private static final Map<String, HttpStatus> PATH_STATUS_CODE_MAPPING = STATUS_CODES.stream().collect(toImmutableMap(status -> "/status/%d".formatted(status.code()), identity()));

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws IOException
        {
            String path = req.getPathInfo();
            if (PATH_STATUS_CODE_MAPPING.containsKey(path)) {
                HttpStatus status = PATH_STATUS_CODE_MAPPING.get(path);
                resp.setStatus(status.code());
                resp.getWriter().write(RESPONSE_TEMPLATE.formatted(status.reason(), path));
            }
            else {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        }
    }
}
