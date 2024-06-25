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

import com.google.common.base.Throwables;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static java.util.Objects.requireNonNull;

class StreamingResponseHandler
        implements ResponseHandler<Void, RuntimeException>
{
    private final AsyncResponse asyncResponse;
    private final RequestLoggingSession requestLoggingSession;
    private final AtomicBoolean hasBeenResumed = new AtomicBoolean(false);

    StreamingResponseHandler(AsyncResponse asyncResponse, RequestLoggingSession requestLoggingSession)
    {
        this.asyncResponse = requireNonNull(asyncResponse, "asyncResponse is null");
        this.requestLoggingSession = requireNonNull(requestLoggingSession, "requestLoggingSession is null");
    }

    @Override
    public Void handleException(Request request, Exception exception)
            throws RuntimeException
    {
        requestLoggingSession.logException(exception);
        requestLoggingSession.close();

        resume(exception);
        return null;
    }

    @Override
    public Void handle(Request request, Response response)
            throws RuntimeException
    {
        StreamingOutput streamingOutput = output -> {
            InputStream inputStream = response.getInputStream();

            // HttpClient/Jersey timeouts control behavior. The configured HttpClient idle timeout
            // controls whether the InputStream will time out. Jersey configuration controls
            // OutputStream and general request timeouts.
            inputStream.transferTo(output);
            output.flush();
        };

        jakarta.ws.rs.core.Response.ResponseBuilder responseBuilder = jakarta.ws.rs.core.Response.status(response.getStatusCode());
        if (HttpStatus.familyForStatusCode(response.getStatusCode()) == HttpStatus.Family.SUCCESSFUL) {
            responseBuilder.entity(streamingOutput);
        }
        response.getHeaders()
                .keySet()
                .stream()
                .map(HeaderName::toString)
                .forEach(name -> response.getHeaders(name).forEach(value -> responseBuilder.header(name, value)));

        requestLoggingSession.logProperty("response.status", response.getStatusCode());
        requestLoggingSession.logProperty("response.headers", response.getHeaders());

        // this will block until StreamingOutput completes

        resume(responseBuilder.build());

        return null;
    }

    @SuppressWarnings("ThrowableNotThrown")
    private void resume(Object result)
    {
        switch (result) {
            case WebApplicationException exception -> resume(exception.getResponse());
            case Throwable exception when Throwables.getRootCause(exception) instanceof WebApplicationException webApplicationException -> resume(webApplicationException.getResponse());
            case Throwable exception -> resume(jakarta.ws.rs.core.Response.status(INTERNAL_SERVER_ERROR.getStatusCode(), Optional.ofNullable(exception.getMessage()).orElse("Unknown error")).build());
            default -> {
                if (hasBeenResumed.compareAndSet(false, true)) {
                    asyncResponse.resume(result);
                }
                else {
                    throw new WebApplicationException("Could not resume with response: " + result, INTERNAL_SERVER_ERROR);
                }
            }
        }
    }
}
