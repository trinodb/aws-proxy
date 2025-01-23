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

import io.trino.aws.proxy.spi.credentials.Credentials;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetResourcePoliciesRequest;
import software.amazon.awssdk.services.glue.model.GetResourcePoliciesResponse;
import software.amazon.awssdk.services.glue.model.GluePolicy;

import java.net.URI;

import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.DATABASE_1;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.DATABASE_2;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.POLICY_A;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.POLICY_B;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TestGlueBase<T extends TestingGlueContext>
{
    protected final GlueClient glueClient;
    protected final T context;

    protected TestGlueBase(TrinoGlueConfig config, Credentials testingCredentials, T context)
    {
        this.context = requireNonNull(context, "context is null");

        URI uri = UriBuilder.fromUri(context.baseUrl()).path(config.getGluePath()).build();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey());

        glueClient = GlueClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                .region(Region.US_EAST_1)
                .endpointOverride(uri)
                .build();
    }

    protected T context()
    {
        return context;
    }

    @PreDestroy
    public void shutdown()
    {
        glueClient.close();
    }

    @Test
    public void testRequests()
    {
        GetDatabasesResponse databases = glueClient.getDatabases(GetDatabasesRequest.builder().build());
        assertThat(databases.databaseList())
                .extracting(Database::name)
                .containsExactlyInAnyOrder(DATABASE_1, DATABASE_2);

        GetResourcePoliciesResponse resourcePolicies = glueClient.getResourcePolicies(GetResourcePoliciesRequest.builder().build());
        assertThat(resourcePolicies.getResourcePoliciesResponseList())
                .extracting(GluePolicy::policyInJson)
                .containsExactlyInAnyOrder(POLICY_A, POLICY_B);
    }
}
