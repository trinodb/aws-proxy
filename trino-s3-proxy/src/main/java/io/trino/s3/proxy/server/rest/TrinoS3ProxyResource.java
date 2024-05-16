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
import io.trino.s3.proxy.server.credentials.SigningController;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

@Path(TrinoS3ProxyRestConstants.S3_PATH)
public class TrinoS3ProxyResource
{
    private final SigningController signingController;

    @Inject
    public TrinoS3ProxyResource(SigningController signingController)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response s3Get(@Context ContainerRequest request)
    {
        return s3Get(request, "");
    }

    @GET
    @Path("{bucket:.*}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response s3Get(@Context ContainerRequest request, @PathParam("bucket") String bucket)
    {
        validateRequest(request);
        return Response.ok().build();
    }

    @HEAD
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response s3Head(@Context ContainerRequest request)
    {
        return s3Head(request, "");
    }

    @HEAD
    @Path("{bucket}/{remainingPath:.*}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response s3Head(@Context ContainerRequest request, @PathParam("bucket") String bucket)
    {
        validateRequest(request);
        return Response.ok().build();
    }

    private void validateRequest(ContainerRequest request)
    {
        String encodedPath = "/" + firstNonNull(request.getPath(false), "");
        String encodedQuery = firstNonNull(request.getUriInfo().getRequestUri().getRawQuery(), "");

        if (!signingController.validateRequest(request.getMethod(), request.getRequestHeaders(), encodedPath, encodedQuery)) {
            // TODO logging, etc.
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }
    }
}
