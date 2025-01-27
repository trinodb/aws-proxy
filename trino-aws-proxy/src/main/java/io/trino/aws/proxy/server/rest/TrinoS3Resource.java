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

import com.google.inject.Inject;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.rest.ResourceSecurity.S3;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import java.util.Optional;

import static io.trino.aws.proxy.server.rest.RequestBuilder.fromRequest;
import static java.util.Objects.requireNonNull;

@ResourceSecurity(S3.class)
public class TrinoS3Resource
{
    private final TrinoS3ProxyClient proxyClient;
    private final Optional<String> serverHostName;
    private final String s3Path;

    @Inject
    public TrinoS3Resource(TrinoS3ProxyClient proxyClient, TrinoAwsProxyConfig trinoS3ProxyConfig)
    {
        this.proxyClient = requireNonNull(proxyClient, "proxyClient is null");
        this.serverHostName = trinoS3ProxyConfig.getS3HostName();

        s3Path = trinoS3ProxyConfig.getS3Path();
    }

    @GET
    public void s3Get(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @GET
    @Path("{path:.*}")
    public void s3GetWithPath(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @HEAD
    public void s3Head(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @HEAD
    @Path("{path:.*}")
    public void s3HeadWithPath(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @PUT
    public void s3Put(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @PUT
    @Path("{path:.*}")
    public void s3PutWithPath(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @POST
    public void s3Post(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @POST
    @Path("{path:.*}")
    public void s3PostWithPath(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @DELETE
    public void s3Delete(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    @DELETE
    @Path("{path:.*}")
    public void s3DeleteWithPath(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession, @Suspended AsyncResponse asyncResponse)
    {
        handler(request, signingMetadata, requestLoggingSession, asyncResponse);
    }

    private void handler(Request request, SigningMetadata signingMetadata, RequestLoggingSession requestLoggingSession, AsyncResponse asyncResponse)
    {
        try {
            ParsedS3Request parsedS3Request = parseRequest(request);

            requestLoggingSession.logProperty("request.parsed.bucket", parsedS3Request.bucketName());
            requestLoggingSession.logProperty("request.parsed.key", parsedS3Request.keyInBucket());
            requestLoggingSession.logProperty("request.emulated.key", signingMetadata.credentials().emulated().secretKey());

            proxyClient.proxyRequest(signingMetadata, parsedS3Request, asyncResponse, requestLoggingSession);
        }
        catch (Throwable e) {
            requestLoggingSession.logException(e);
            throw e;
        }
    }

    private ParsedS3Request parseRequest(Request request)
    {
        String path = request.requestUri().getRawPath();
        if (!path.startsWith(s3Path)) {
            // Sanity check: this should never happen as this resource is prefixed at build time
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        path = path.substring(s3Path.length());
        if (path.isEmpty()) {
            path = "/";
        }
        else if ((path.length() > 1) && path.startsWith("/")) {
            path = path.substring(1);
        }

        return fromRequest(request, path, serverHostName);
    }
}
