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
import io.trino.s3.proxy.server.signing.SigningServiceType;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import org.glassfish.jersey.server.ContainerRequest;

import java.util.Optional;

import static io.trino.s3.proxy.server.rest.RequestBuilder.fromRequest;
import static java.util.Objects.requireNonNull;

public class TrinoS3ProxyResource
{
    private final SigningController signingController;
    private final TrinoS3ProxyClient proxyClient;
    private final Optional<String> serverHostName;

    @Inject
    public TrinoS3ProxyResource(SigningController signingController, TrinoS3ProxyClient proxyClient, TrinoS3ProxyConfig trinoS3ProxyConfig)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.proxyClient = requireNonNull(proxyClient, "proxyClient is null");
        this.serverHostName = trinoS3ProxyConfig.getS3HostName();
    }

    @GET
    public void s3Get(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        s3Get(containerRequest, asyncResponse, "/");
    }

    @GET
    @Path("{path:.*}")
    public void s3Get(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        Request request = fromRequest(containerRequest);
        proxyClient.proxyRequest(signingController.validateAndParseAuthorization(request, SigningServiceType.S3), parseRequest(path, request), asyncResponse);
    }

    @HEAD
    public void s3Head(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        s3Head(containerRequest, asyncResponse, "/");
    }

    @HEAD
    @Path("{path:.*}")
    public void s3Head(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        Request request = fromRequest(containerRequest);
        proxyClient.proxyRequest(signingController.validateAndParseAuthorization(request, SigningServiceType.S3), parseRequest(path, request), asyncResponse);
    }

    @PUT
    public void s3Put(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        s3Put(containerRequest, asyncResponse, "/");
    }

    @PUT
    @Path("{path:.*}")
    public void s3Put(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        Request request = fromRequest(containerRequest);
        proxyClient.proxyRequest(signingController.validateAndParseAuthorization(request, SigningServiceType.S3), parseRequest(path, request), asyncResponse);
    }

    @POST
    public void s3Post(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        s3Post(containerRequest, asyncResponse, "/");
    }

    @POST
    @Path("{path:.*}")
    public void s3Post(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        Request request = fromRequest(containerRequest);
        proxyClient.proxyRequest(signingController.validateAndParseAuthorization(request, SigningServiceType.S3), parseRequest(path, request), asyncResponse);
    }

    @DELETE
    public void s3Delete(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse)
    {
        s3Delete(containerRequest, asyncResponse, "/");
    }

    @DELETE
    @Path("{path:.*}")
    public void s3Delete(@Context ContainerRequest containerRequest, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        Request request = fromRequest(containerRequest);
        proxyClient.proxyRequest(signingController.validateAndParseAuthorization(request, SigningServiceType.S3), parseRequest(path, request), asyncResponse);
    }

    private ParsedS3Request parseRequest(String path, Request request)
    {
        return fromRequest(request, path, serverHostName);
    }
}
