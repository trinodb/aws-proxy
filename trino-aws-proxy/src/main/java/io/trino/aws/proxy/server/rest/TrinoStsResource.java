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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.rest.AssumeRoleResponse.AssumeRoleResult;
import io.trino.aws.proxy.server.rest.AssumeRoleResponse.AssumedRoleUser;
import io.trino.aws.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.aws.proxy.spi.credentials.EmulatedAssumedRole;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import io.trino.aws.proxy.spi.util.MultiMap;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.uri.UriComponent;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class TrinoStsResource
{
    private static final Logger log = Logger.get(TrinoStsResource.class);

    private final AssumedRoleProvider assumedRoleProvider;
    private final XmlMapper xmlMapper;

    @Inject
    public TrinoStsResource(AssumedRoleProvider assumedRoleProvider, XmlMapper xmlMapper)
    {
        this.assumedRoleProvider = requireNonNull(assumedRoleProvider, "assumedRoleProvider is null");
        this.xmlMapper = requireNonNull(xmlMapper, "xmlMapper is null");
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    @POST
    public Response post(@Context Request request, @Context SigningMetadata signingMetadata, @Context RequestLoggingSession requestLoggingSession)
    {
        Map<String, String> arguments = deserializeRequest(request.requestQueryParameters(), request.requestContent().standardBytes());

        String action = Optional.ofNullable(arguments.get("Action")).orElse("");

        return switch (action) {
            case "AssumeRole" -> assumeRole(request.requestAuthorization().region(), signingMetadata, arguments, requestLoggingSession);
            default -> {
                log.debug("Request missing \"Action\". Arguments: %s", arguments);
                requestLoggingSession.logError("request.action.unsupported", arguments);
                yield Response.status(Response.Status.BAD_REQUEST).build();
            }
        };
    }

    private Response assumeRole(String region, SigningMetadata signingMetadata, Map<String, String> arguments, RequestLoggingSession requestLoggingSession)
    {
        String roleArn = arguments.get("RoleArn");
        if (roleArn == null) {
            log.debug("Request missing \"RoleArn\". Arguments: %s", arguments);
            requestLoggingSession.logError("request.role-arn.missing", arguments);
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Optional<String> roleSessionName = Optional.ofNullable(arguments.get("RoleSessionName"));
        Optional<String> externalId = Optional.ofNullable(arguments.get("ExternalId"));
        Optional<Integer> durationSeconds = Optional.ofNullable(arguments.get("DurationSeconds")).map(TrinoStsResource::mapToInt);

        EmulatedAssumedRole assumedRole = assumedRoleProvider.assumeEmulatedRole(signingMetadata.credentials().emulated(), region, roleArn, externalId, roleSessionName, durationSeconds)
                .orElseThrow(() -> {
                    log.debug("Assume role failed. Arguments: %s", arguments);
                    requestLoggingSession.logError("request.assume-role.failure", arguments);
                    return new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED).build());
                });

        String assumedRoleSession = assumedRole.emulatedCredential().session().orElseThrow(() -> {
            log.debug("Assume role returned an illegal response - no session was created. Arguments: %s", arguments);
            requestLoggingSession.logError("request.assume-role.illegal-response", arguments);
            return new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED).build());
        });
        String expiration = AwsTimestamp.toResponseFormat(assumedRole.expiration());
        AssumeRoleResult response = new AssumeRoleResult(
                new AssumedRoleUser(assumedRole.arn(), assumedRole.roleId()),
                new AssumeRoleResponse.Credentials(assumedRole.emulatedCredential().accessKey(), assumedRole.emulatedCredential().secretKey(), assumedRoleSession, expiration));

        String xml;
        try {
            // use our custom mapper which conforms to AWS standards
            xml = xmlMapper.writeValueAsString(new AssumeRoleResponse(response));
        }
        catch (JsonProcessingException e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        requestLoggingSession.logProperty("response.assume-role.arn", response.assumedRoleUser().arn());
        requestLoggingSession.logProperty("response.assume-role.role-id", response.assumedRoleUser().assumedRoleId());
        requestLoggingSession.logProperty("response.assume-role.access-key", response.credentials().accessKeyId());
        requestLoggingSession.logProperty("response.assume-role.expiration", response.credentials().expiration());
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
