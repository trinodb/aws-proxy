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
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningController.SigningIdentity;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.Optional;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static java.util.Objects.requireNonNull;

public class SecurityFilter
        implements ContainerRequestFilter, ContainerResponseFilter
{
    private static final Logger log = Logger.get(SecurityFilter.class);

    private final SigningController signingController;
    private final SigningServiceType signingServiceType;
    private final RequestLoggerController requestLoggerController;

    public SecurityFilter(SigningController signingController, SigningServiceType signingServiceType, RequestLoggerController requestLoggerController)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.signingServiceType = requireNonNull(signingServiceType, "signingServiceType is null");
        this.requestLoggerController = requireNonNull(requestLoggerController, "requestLoggerController is null");
    }

    @SuppressWarnings("ThrowableNotThrown")
    @Override
    public void filter(ContainerRequestContext requestContext)
            throws IOException
    {
        if (requestContext.getRequest() instanceof ContainerRequest containerRequest) {
            if (containerRequest.getUriInfo().getMatchedResourceMethod() == null) {
                log.warn("%s does not have a MatchedResourceMethod", containerRequest.getRequestUri());
                throw new WebApplicationException(INTERNAL_SERVER_ERROR);
            }

            Request request = RequestBuilder.fromRequest(containerRequest);
            containerRequest.setProperty(Request.class.getName(), request);

            RequestLoggingSession requestLoggingSession = requestLoggerController.newRequestSession(request, signingServiceType);
            containerRequest.setProperty(RequestLoggingSession.class.getName(), requestLoggingSession);

            SigningIdentity signingIdentity;
            try {
                signingIdentity = signingController.validateAndParseAuthorization(request, signingServiceType);
            }
            catch (Exception e) {
                requestLoggingSession.logException(e);

                switch (Throwables.getRootCause(e)) {
                    case WebApplicationException webApplicationException -> throw webApplicationException;
                    case IOException ioException -> throw ioException;
                    case RuntimeException runtimeException -> throw runtimeException;
                    default -> throw new RuntimeException(e);
                }
            }

            containerRequest.setProperty(SigningMetadata.class.getName(), signingIdentity.signingMetadata());
            signingIdentity.identity().ifPresent(identity -> containerRequest.setProperty(Identity.class.getName(), identity));
        }
        else {
            log.warn("%s is not a ContainerRequest", requestContext.getRequest().getClass().getName());
            throw new WebApplicationException(INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
            throws IOException
    {
        if ((requestContext.getRequest() instanceof ContainerRequest containerRequest) && (responseContext instanceof ContainerResponse containerResponse)) {
            Optional.ofNullable(unwrap(containerRequest, RequestLoggingSession.class))
                    .ifPresent(requestLoggingSession -> {
                        OutputStream entityStream = (containerResponse.isCommitted() || !containerResponse.hasEntity()) ? null : responseContext.getEntityStream();
                        if (entityStream != null) {
                            responseContext.setEntityStream(closingStream(requestLoggingSession, entityStream));
                        }
                        else {
                            requestLoggingSession.close();
                        }
                    });
        }
    }

    private static OutputStream closingStream(Closeable closeable, OutputStream delegate)
    {
        return new OutputStream()
        {
            @Override
            public void write(int b)
                    throws IOException
            {
                delegate.write(b);
            }

            @Override
            public void write(byte[] b)
                    throws IOException
            {
                delegate.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len)
                    throws IOException
            {
                delegate.write(b, off, len);
            }

            @Override
            public void flush()
                    throws IOException
            {
                delegate.flush();
            }

            @Override
            public void close()
                    throws IOException
            {
                try (closeable) {
                    delegate.close();
                }
            }
        };
    }

    static Object unwrapType(ContainerRequest containerRequest, Type type)
    {
        return containerRequest.getProperty(type.getTypeName());
    }

    @SuppressWarnings("unchecked")
    static <T> T unwrap(ContainerRequest containerRequest, Class<T> clazz)
    {
        return (T) containerRequest.getProperty(clazz.getTypeName());
    }
}
