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

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.SigningController;
import io.trino.s3.proxy.server.credentials.SigningMetadata;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Path(TrinoS3ProxyRestConstants.S3_PATH)
public class TrinoS3ProxyResource
{
    private final SigningController signingController;
    private final TrinoS3ProxyClient proxyClient;

    @Inject
    public TrinoS3ProxyResource(SigningController signingController, TrinoS3ProxyClient proxyClient)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.proxyClient = requireNonNull(proxyClient, "proxyClient is null");
    }

    @GET
    public void s3Get(@Context ContainerRequest request, @Suspended AsyncResponse asyncResponse)
    {
        s3Get(request, asyncResponse, "/");
    }

    @GET
    @Path("{path:.*}")
    public void s3Get(@Context ContainerRequest request, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        proxyClient.proxyRequest(validateAndParseAuthorization(request), request, asyncResponse, getBucket(path));
    }

    @HEAD
    public void s3Head(@Context ContainerRequest request, @Suspended AsyncResponse asyncResponse)
    {
        s3Head(request, asyncResponse, "/");
    }

    @HEAD
    @Path("{path:.*}")
    public void s3Head(@Context ContainerRequest request, @Suspended AsyncResponse asyncResponse, @PathParam("path") String path)
    {
        proxyClient.proxyRequest(validateAndParseAuthorization(request), request, asyncResponse, getBucket(path));
    }

    private String getBucket(String path)
    {
        List<String> parts = Splitter.on("/").splitToList(path);
        return parts.isEmpty() ? "" : parts.getFirst();
    }

    private SigningMetadata validateAndParseAuthorization(ContainerRequest request)
    {
        return signingController.signingMetadataFromRequest(
                        Credentials::emulated,
                        request.getRequestUri(),
                        request.getRequestHeaders(),
                        request.getUriInfo().getQueryParameters(),
                        request.getMethod(),
                        request.getPath(false))
                .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));
    }
}
