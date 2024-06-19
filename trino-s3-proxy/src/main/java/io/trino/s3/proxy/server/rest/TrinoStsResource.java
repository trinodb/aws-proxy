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
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.s3.proxy.server.rest.AssumeRoleResponse.AssumeRoleResult;
import io.trino.s3.proxy.server.rest.AssumeRoleResponse.AssumedRoleUser;
import io.trino.s3.proxy.spi.collections.MultiMap;
import io.trino.s3.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.s3.proxy.spi.credentials.EmulatedAssumedRole;
import io.trino.s3.proxy.spi.rest.Request;
import io.trino.s3.proxy.spi.signing.SigningController;
import io.trino.s3.proxy.spi.signing.SigningMetadata;
import io.trino.s3.proxy.spi.signing.SigningServiceType;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.uri.UriComponent;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.s3.proxy.server.rest.RequestBuilder.fromRequest;
import static java.util.Objects.requireNonNull;

public class TrinoStsResource
{
    private static final Logger log = Logger.get(TrinoStsResource.class);

    private final SigningController signingController;
    private final AssumedRoleProvider assumedRoleProvider;
    private final XmlMapper xmlMapper;

    @Inject
    public TrinoStsResource(SigningController signingController, AssumedRoleProvider assumedRoleProvider, XmlMapper xmlMapper)
    {
        this.signingController = requireNonNull(signingController, "signingController is null");
        this.assumedRoleProvider = requireNonNull(assumedRoleProvider, "assumedRoleProvider is null");
        this.xmlMapper = requireNonNull(xmlMapper, "xmlMapper is null");
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    @POST
    public Response post(@Context ContainerRequest containerRequest)
    {
        Request request = fromRequest(containerRequest);
        SigningMetadata signingMetadata = signingController.validateAndParseAuthorization(request, SigningServiceType.STS);
        Map<String, String> arguments = deserializeRequest(request.requestQueryParameters(), request.requestContent().standardBytes());

        String action = Optional.ofNullable(arguments.get("Action")).orElse("");

        return switch (action) {
            case "AssumeRole" -> assumeRole(request.requestAuthorization().region(), signingMetadata, arguments);
            default -> {
                log.debug("Request missing \"Action\". Arguments: %s", arguments);
                yield Response.status(Response.Status.BAD_REQUEST).build();
            }
        };
    }

    private Response assumeRole(String region, SigningMetadata signingMetadata, Map<String, String> arguments)
    {
        String roleArn = arguments.get("RoleArn");
        if (roleArn == null) {
            log.debug("Request missing \"RoleArn\". Arguments: %s", arguments);
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Optional<String> roleSessionName = Optional.ofNullable(arguments.get("RoleSessionName"));
        Optional<String> externalId = Optional.ofNullable(arguments.get("ExternalId"));
        Optional<Integer> durationSeconds = Optional.ofNullable(arguments.get("DurationSeconds")).map(TrinoStsResource::mapToInt);

        EmulatedAssumedRole assumedRole = assumedRoleProvider.assumeEmulatedRole(signingMetadata.credentials().emulated(), region, roleArn, externalId, roleSessionName, durationSeconds)
                .orElseThrow(() -> {
                    log.debug("Assume role failed. Arguments: %s", arguments);
                    return new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED).build());
                });

        String expiration = signingController.formatResponseInstant(assumedRole.expiration());
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

    private Map<String, String> deserializeRequest(MultiMap queryParameters, Optional<byte[]> maybeEntity)
    {
        return maybeEntity
                .map(entity -> UriComponent.decodeQuery(new String(entity, StandardCharsets.UTF_8), true).entrySet().stream())
                .orElseGet(() -> queryParameters.entrySet().stream())
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().getFirst()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
