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
package io.trino.aws.proxy.glue.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.aws.proxy.glue.handler.GlueRequestHandler;
import io.trino.aws.proxy.server.rest.RequestLoggingSession;
import io.trino.aws.proxy.server.rest.ResourceSecurity;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.InternalServiceException;

import java.io.InputStream;

import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.protocols.json.internal.unmarshall.JsonErrorCodeParser.X_AMZN_ERROR_TYPE;

@ResourceSecurity(GlueResourceSecurity.class)
public class TrinoGlueResource
{
    private final ObjectMapper objectMapper;
    private final GlueRequestHandler requestHandler;
    private final ModelLoader modelLoader;

    @Inject
    public TrinoGlueResource(ObjectMapper objectMapper, GlueRequestHandler requestHandler, ModelLoader modelLoader)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.requestHandler = requireNonNull(requestHandler, "requestHandler is null");
        this.modelLoader = requireNonNull(modelLoader, "modelLoader is null");
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response gluePost(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession)
    {
        requestLoggingSession.logProperty("request.glue.emulated.key", signingMetadata.credentials().emulated().secretKey());

        String target = request.requestHeaders().unmodifiedHeaders().getFirst("x-amz-target")
                .orElseThrow(() -> new WebApplicationException(BAD_REQUEST));

        requestLoggingSession.logProperty("request.glue.target", target);

        Class<?> requestClass = modelLoader.requestClass(target)
                .orElseThrow(() -> new WebApplicationException(NOT_FOUND));
        GlueRequest glueRequest = unmarshal(request, requestClass, objectMapper);

        ParsedGlueRequest parsedGlueRequest = new ParsedGlueRequest(
                request.requestId(),
                request.requestAuthorization(),
                request.requestDate(),
                target,
                glueRequest,
                request.requestHeaders(),
                request.requestQueryParameters());

        GlueResponse glueResponse;
        try {
            glueResponse = requestHandler.handleRequest(parsedGlueRequest, signingMetadata, requestLoggingSession);
        }
        catch (GlueException e) {
            requestLoggingSession.logException(e);

            return Response.status(BAD_REQUEST)
                    .header(X_AMZN_ERROR_TYPE, e.getClass().getSimpleName() + ":" + e.getMessage())
                    .build();
        }
        catch (Exception e) {
            requestLoggingSession.logException(e);
            return Response.status(INTERNAL_SERVER_ERROR)
                    .header(X_AMZN_ERROR_TYPE, InternalServiceException.class.getSimpleName() + ":" + e.getMessage())
                    .build();
        }

        return Response.ok(glueResponse).build();
    }

    private GlueRequest unmarshal(Request request, Class<?> clazz, ObjectMapper objectMapper)
    {
        try {
            InputStream inputStream = request.requestContent().inputStream().orElseThrow(() -> new WebApplicationException(BAD_REQUEST));
            return (GlueRequest) objectMapper.readValue(inputStream, clazz);
        }
        catch (Exception e) {
            throw new WebApplicationException(BAD_REQUEST);
        }
    }
}
