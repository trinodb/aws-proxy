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
package io.trino.aws.proxy.glue;

import com.google.common.collect.ImmutableList;
import io.trino.aws.proxy.glue.handler.GlueRequestHandler;
import io.trino.aws.proxy.glue.rest.ParsedGlueRequest;
import io.trino.aws.proxy.server.rest.RequestLoggingSession;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import jakarta.ws.rs.WebApplicationException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetResourcePoliciesRequest;
import software.amazon.awssdk.services.glue.model.GetResourcePoliciesResponse;
import software.amazon.awssdk.services.glue.model.GluePolicy;
import software.amazon.awssdk.services.glue.model.GlueResponse;

import java.util.Collection;
import java.util.Map;

import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestingGlueRequestHandler
        implements GlueRequestHandler
{
    public static final String POLICY_A = """
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "glue:GetDatabase",
                        "Resource": "arn:aws:glue:us-east-1:123456789012:database/d1"
                    }
                ]
            }
            """;

    public static final String POLICY_B = """
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "glue:GetDatabase",
                        "Resource": "arn:aws:glue:us-east-1:123456789012:database/d1"
                    }
                ]
            }
            """;

    public static final String DATABASE_1 = "db1";
    public static final String DATABASE_2 = "db2";

    @Override
    public GlueResponse handleRequest(ParsedGlueRequest request, SigningMetadata signingMetadata, RequestLoggingSession requestLoggingSession)
    {
        return switch (request.glueRequest()) {
            case GetDatabasesRequest _ -> GetDatabasesResponse.builder()
                    .databaseList(Database.builder().name(DATABASE_1).build(), Database.builder().name(DATABASE_2).build())
                    .build();

            case GetResourcePoliciesRequest _ -> GetResourcePoliciesResponse.builder()
                    .getResourcePoliciesResponseList(gluePolicies())
                    .build();

            case CreateDatabaseRequest databaseRequest -> {
                assertThat(databaseRequest.catalogId()).isEqualTo("1");
                assertThat(databaseRequest.databaseInput().name()).isEqualTo("database1");
                assertThat(databaseRequest.databaseInput().description()).isEqualTo("just another description for a test db");
                assertThat(databaseRequest.databaseInput().locationUri()).isEqualTo("dummy locationUri");
                assertThat(databaseRequest.databaseInput().parameters()).isEqualTo(Map.of("k1", "v1"));
                yield CreateDatabaseResponse.builder().build();
            }

            case CreateTableRequest tableRequest -> {
                assertThat(tableRequest.catalogId()).isEqualTo("1");
                assertThat(tableRequest.databaseName()).isEqualTo("database1");
                assertThat(tableRequest.tableInput().description()).isEqualTo("desc1");
                assertThat(tableRequest.tableInput().name()).isEqualTo("tableName");
                assertThat(tableRequest.tableInput().parameters()).isEqualTo(Map.of("k1", "v1"));
                yield CreateDatabaseResponse.builder().build();
            }

            default -> throw new WebApplicationException(NOT_FOUND);
        };
    }

    private static Collection<GluePolicy> gluePolicies()
    {
        return ImmutableList.of(
                GluePolicy.builder()
                        .policyInJson(POLICY_A)
                        .build(),
                GluePolicy.builder()
                        .policyInJson(POLICY_B)
                        .build());
    }
}
