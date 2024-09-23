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

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.inject.Inject;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.rest.Request;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ResourceContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.glassfish.jersey.server.ContainerRequest;

import java.util.Optional;
import java.util.UUID;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_XML_TYPE;
import static java.util.Objects.requireNonNull;

public class ThrowableMapper
        implements ExceptionMapper<Throwable>
{
    private static final Logger log = Logger.get(ThrowableMapper.class);
    private static final String X_AMZ_REQUEST_ID = "x-amz-request-id";

    @Context
    private ResourceContext resourceContext;

    private final XmlMapper xmlMapper;

    @Inject
    public ThrowableMapper(XmlMapper xmlMapper)
    {
        this.xmlMapper = requireNonNull(xmlMapper, "xmlMapper is null");
    }

    @Override
    public Response toResponse(Throwable throwable)
    {
        ContainerRequest containerRequest = resourceContext.getResource(ContainerRequest.class);
        Optional<String> requestId = Optional.ofNullable((Request) containerRequest.getProperty(Request.class.getName()))
                .map(Request::requestId)
                .map(UUID::toString);

        HttpStatus status = switch (throwable) {
            case WebApplicationException webApplicationException -> HttpStatus.fromStatusCode(webApplicationException.getResponse().getStatus());
            default -> {
                log.error(throwable, "Request failed for %s", containerRequest.getRequestUri());
                yield HttpStatus.INTERNAL_SERVER_ERROR;
            }
        };

        try {
            ErrorResponse response = new ErrorResponse(
                                status.reason(),
                                Optional.ofNullable(throwable.getMessage()),
                                containerRequest.getRequestUri().getPath(),
                                requestId);

            ResponseBuilder responseBuilder = Response.status(status.code())
                    .header(CONTENT_TYPE, APPLICATION_XML_TYPE);
            requestId.ifPresent(id -> responseBuilder.header(X_AMZ_REQUEST_ID, id));
            return responseBuilder.entity(xmlMapper.writeValueAsString(response)).build();
        }
        catch (Exception exception) {
            log.error(exception, "Processing of throwable %s caused an exception", throwable);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }
}
