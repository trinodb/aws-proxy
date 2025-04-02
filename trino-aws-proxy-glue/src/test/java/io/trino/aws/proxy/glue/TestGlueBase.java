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

import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.CompressionConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseIdentifier;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.FederatedDatabase;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetResourcePoliciesRequest;
import software.amazon.awssdk.services.glue.model.GetResourcePoliciesResponse;
import software.amazon.awssdk.services.glue.model.GluePolicy;
import software.amazon.awssdk.services.glue.model.IcebergInput;
import software.amazon.awssdk.services.glue.model.MetadataOperation;
import software.amazon.awssdk.services.glue.model.OpenTableFormatInput;
import software.amazon.awssdk.services.glue.model.PartitionIndex;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableIdentifier;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.DATABASE_1;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.DATABASE_2;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.NOW;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.POLICY_A;
import static io.trino.aws.proxy.glue.TestingGlueRequestHandler.POLICY_B;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

public abstract class TestGlueBase<T extends TestingGlueContext>
{
    protected final GlueClient glueClient;
    protected final T context;

    protected TestGlueBase(TrinoGlueConfig config, IdentityCredential testingCredentials, T context)
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
        assertThat(databases.databaseList().getFirst().createTime())
                .isCloseTo(NOW, within(1, ChronoUnit.SECONDS));

        GetResourcePoliciesResponse resourcePolicies = glueClient.getResourcePolicies(GetResourcePoliciesRequest.builder().build());
        assertThat(resourcePolicies.getResourcePoliciesResponseList())
                .extracting(GluePolicy::policyInJson)
                .containsExactlyInAnyOrder(POLICY_A, POLICY_B);
    }

    @Test
    public void testComplexSerialization()
    {
        String catalogId = "1";
        String databaseName = "database1";

        glueClient.createDatabase(
                CreateDatabaseRequest.builder()
                        .catalogId(catalogId)
                        .databaseInput(DatabaseInput.builder()
                                .name(databaseName)
                                .description("just another description for a test db")
                                .locationUri("dummy locationUri")
                                .parameters(Map.of("k1", "v1"))
                                .targetDatabase(DatabaseIdentifier.builder()
                                        .catalogId(catalogId)
                                        .databaseName(databaseName)
                                        .region("us-east-1")
                                        .build())
                                .federatedDatabase(FederatedDatabase.builder().identifier("iden1").build())
                                .build())
                        .build());

        glueClient.createTable(CreateTableRequest.builder()
                .catalogId(catalogId)
                .databaseName(databaseName)
                .tableInput(TableInput.builder()
                        .description("desc1")
                        .name("tableName")
                        .parameters(Map.of("k1", "v1"))
                        .owner("user1")
                        .partitionKeys(Column.builder().name("col1").type("type1").build())
                        .storageDescriptor(StorageDescriptor.builder().build())
                        .retention(1)
                        .targetTable(TableIdentifier.builder()
                                .catalogId(catalogId)
                                .databaseName(databaseName)
                                .name("targetTable1")
                                .build())
                        .build())
                .openTableFormatInput(OpenTableFormatInput.builder()
                        .icebergInput(IcebergInput.builder()
                                .metadataOperation(MetadataOperation.CREATE)
                                .version("1")
                                .build())
                        .build())
                .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                        .compressionConfiguration(CompressionConfiguration.builder()
                                .requestCompressionEnabled(true)
                                .build())
                        .build())
                .partitionIndexes(PartitionIndex.builder().indexName("i1").keys("k1").build())
                .transactionId("t1")
                .build());
    }

    @Test
    public void testNotFoundException()
    {
        assertThatThrownBy(() -> glueClient.getDatabase(
                GetDatabaseRequest.builder()
                        .catalogId("1")
                        .name("blahblah")
                        .build()))
                .isInstanceOf(EntityNotFoundException.class);
    }
}
