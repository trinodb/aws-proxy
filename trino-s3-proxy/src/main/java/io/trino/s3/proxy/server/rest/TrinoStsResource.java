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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.s3.proxy.server.credentials.AssumedRoleProvider;
import io.trino.s3.proxy.server.credentials.EmulatedAssumedRole;
import io.trino.s3.proxy.server.credentials.SigningController;
import io.trino.s3.proxy.server.credentials.SigningMetadata;
import io.trino.s3.proxy.server.credentials.SigningServiceType;
import io.trino.s3.proxy.server.rest.AssumeRoleResponse.AssumeRoleResult;
import io.trino.s3.proxy.server.rest.AssumeRoleResponse.AssumedRoleUser;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.uri.UriComponent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.ByteStreams.toByteArray;
import static io.trino.s3.proxy.server.credentials.SigningController.formatResponseInstant;
import static java.util.Objects.requireNonNull;

@Path(TrinoS3ProxyRestConstants.STS_PATH)
public class TrinoStsResource
{
    private static final Logger log = Logger.get(TrinoStsResource.class);

    private final SigningController signingController;
    private final AssumedRoleProvider assumedRoleProvider;
    private final ObjectMapper xmlMapper;

    @Inject
    public TrinoStsResource(SigningController signingController, AssumedRoleProvider assumedRoleProvider)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.assumedRoleProvider = requireNonNull(assumedRoleProvider, "assumedRoleProvider is null");

        xmlMapper = new XmlMapper().setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    @POST
    public Response post(@Context ContainerRequest request)
    {
        Optional<byte[]> entity;
        try {
            entity = request.hasEntity() ? Optional.of(toByteArray(request.getEntityStream())) : Optional.empty();
        }
        catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        SigningMetadata signingMetadata = signingController.validateAndParseAuthorization(request, SigningServiceType.STS, entity);
        Map<String, String> arguments = deserializeRequest(request, entity);

        String action = Optional.ofNullable(arguments.get("Action")).orElse("");

        return switch (action) {
            case "AssumeRole" -> assumeRole(request, signingMetadata, arguments, entity);
            default -> {
                log.debug("Request missing \"Action\". Arguments: %s", arguments);
                yield Response.status(Response.Status.BAD_REQUEST).build();
            }
        };
    }

    private Response assumeRole(ContainerRequest request, SigningMetadata signingMetadata, Map<String, String> arguments, Optional<byte[]> entity)
    {
        String roleArn = arguments.get("RoleArn");
        if (roleArn == null) {
            log.debug("Request missing \"RoleArn\". Arguments: %s", arguments);
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Optional<String> roleSessionName = Optional.ofNullable(arguments.get("RoleSessionName"));
        Optional<String> externalId = Optional.ofNullable(arguments.get("ExternalId"));
        Optional<Integer> durationSeconds = Optional.ofNullable(arguments.get("DurationSeconds")).map(TrinoStsResource::mapToInt);

        EmulatedAssumedRole assumedRole = assumedRoleProvider.assumeEmulatedRole(signingMetadata, roleArn, externalId, roleSessionName, durationSeconds)
                .orElseThrow(() -> {
                    log.debug("Assume role failed. Arguments: %s", arguments);
                    return new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED).build());
                });

        String expiration = formatResponseInstant(assumedRole.expiration());
        AssumeRoleResult response = new AssumeRoleResult(
                new AssumedRoleUser(assumedRole.arn(), assumedRole.roleId()),
                new AssumeRoleResponse.Credentials(assumedRole.credential().accessKey(), assumedRole.credential().secretKey(), assumedRole.session(), expiration));

        String xml;
        try {
            // use our custom mapper which conforms to AWS standards
            xml = xmlMapper.writeValueAsString(new AssumeRoleResponse(response));
        }
        catch (JsonProcessingException e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return Response.ok(xml, MediaType.APPLICATION_XML_TYPE).build();
    }

    private static int mapToInt(String str)
    {
        try {
            return Integer.parseInt(str);
        }
        catch (NumberFormatException e) {
            log.debug("Invalid int value received: %s", str);
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).build());
        }
    }

    private Map<String, String> deserializeRequest(ContainerRequest request, Optional<byte[]> maybeEntity)
    {
        MultivaluedMap<String, String> values = maybeEntity.map(entity -> UriComponent.decodeQuery(new String(entity, StandardCharsets.UTF_8), true))
                .orElseGet(() -> request.getUriInfo().getQueryParameters());
        return values.entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getFirst()));
    }
}
