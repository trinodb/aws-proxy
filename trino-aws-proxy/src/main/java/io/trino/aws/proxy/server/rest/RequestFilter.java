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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.server.spi.internal.ValueParamProvider.Priority.HIGH;

public class RequestFilter
        implements ContainerRequestFilter, ContainerResponseFilter, ValueParamProvider
{
    private final Logger log = Logger.get(RequestFilter.class);
    private final SigningController signingController;
    private final Map<Class<?>, SigningServiceType> signingServiceTypesMap;
    private final RequestLoggerController requestLoggerController;

    private record InternalRequestContext(Request request, SigningMetadata signingMetadata, RequestLoggingSession requestLoggingSession)
    {
        private InternalRequestContext
        {
            requireNonNull(request, "request is null");
            requireNonNull(signingMetadata, "signingMetadata is null");
            requireNonNull(requestLoggingSession, "requestLoggingSession is null");
        }
    }

    @Inject
    RequestFilter(SigningController signingController, Map<Class<?>, SigningServiceType> signingServiceTypesMap, RequestLoggerController requestLoggerController)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.signingServiceTypesMap = ImmutableMap.copyOf(signingServiceTypesMap);
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

            Class<?> declaringClass = containerRequest.getUriInfo().getMatchedResourceMethod().getInvocable().getDefinitionMethod().getDeclaringClass();
            SigningServiceType signingServiceType = signingServiceTypesMap.get(declaringClass);
            if (signingServiceType == null) {
                log.warn("%s does not have a SigningServiceType", declaringClass.getName());
                throw new WebApplicationException(INTERNAL_SERVER_ERROR);
            }

            Request request = RequestBuilder.fromRequest(containerRequest);
            containerRequest.setProperty(Request.class.getName(), request);

            RequestLoggingSession requestLoggingSession = requestLoggerController.newRequestSession(request, signingServiceType);
            containerRequest.setProperty(RequestLoggingSession.class.getName(), requestLoggingSession);

            SigningMetadata signingMetadata;
            try {
                signingMetadata = signingController.validateAndParseAuthorization(request, signingServiceType);
                containerRequest.setProperty(SigningMetadata.class.getName(), signingMetadata);
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

    @Override
    public Function<ContainerRequest, ?> getValueProvider(Parameter parameter)
    {
        if (Request.class.isAssignableFrom(parameter.getRawType()) || SigningMetadata.class.isAssignableFrom(parameter.getRawType()) || RequestLoggingSession.class.isAssignableFrom(parameter.getRawType())) {
            return containerRequest -> unwrap(containerRequest, parameter.getRawType());
        }
        return null;
    }

    @Override
    public PriorityType getPriority()
    {
        return HIGH;
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

    @SuppressWarnings("unchecked")
    private static <T> T unwrap(ContainerRequest containerRequest, Class<T> type)
    {
        return (T) containerRequest.getProperty(type.getName());
    }
}
