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
package io.trino.s3.proxy.server.rest;

import com.google.inject.Inject;
import io.trino.s3.proxy.server.signing.SigningController;
import io.trino.s3.proxy.server.signing.SigningMetadata;
import io.trino.s3.proxy.server.signing.SigningServiceType;
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
import org.glassfish.jersey.server.ContainerRequest;

import java.util.Optional;

import static io.trino.s3.proxy.server.rest.RequestBuilder.fromRequest;
import static java.util.Objects.requireNonNull;

public class TrinoS3ProxyResource
{
    private final SigningController signingController;
    private final TrinoS3ProxyClient proxyClient;
    private final Optional<String> serverHostName;
    private final String s3Path;

    @Inject
    public TrinoS3ProxyResource(SigningController signingController, TrinoS3ProxyClient proxyClient, TrinoS3ProxyConfig trinoS3ProxyConfig)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.proxyClient = requireNonNull(proxyClient, "proxyClient is null");
        this.serverHostName = trinoS3ProxyConfig.getS3HostName();
        s3Path = trinoS3ProxyConfig.getS3Path();
    }

    @GET
    public void s3Get(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @GET
    @Path("{path:.*}")
    public void s3GetWithPath(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @HEAD
    public void s3Head(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @HEAD
    @Path("{path:.*}")
    public void s3HeadWithPath(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @PUT
    public void s3Put(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @PUT
    @Path("{path:.*}")
    public void s3PutWithPath(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @POST
    public void s3Post(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @POST
    @Path("{path:.*}")
    public void s3PostWithPath(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @DELETE
    public void s3Delete(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    @DELETE
    @Path("{path:.*}")
    public void s3DeleteWithPath(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        handler(containerRequest, asyncResponse);
    }

    private void handler(ContainerRequest containerRequest, AsyncResponse asyncResponse)
    {
        Request request = fromRequest(containerRequest);
        ParsedS3Request parsedS3Request = parseRequest(request);
        SigningMetadata signingMetadata = signingController.validateAndParseAuthorization(request, SigningServiceType.S3);

        proxyClient.proxyRequest(signingMetadata, parsedS3Request, asyncResponse);
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
