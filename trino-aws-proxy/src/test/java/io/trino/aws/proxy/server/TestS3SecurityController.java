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
package io.trino.aws.proxy.server;

import com.google.inject.Inject;
import io.trino.aws.proxy.server.testing.TestingS3SecurityController;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static io.trino.aws.proxy.spi.security.SecurityResponse.FAILURE;
import static io.trino.aws.proxy.spi.security.SecurityResponse.SUCCESS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TrinoAwsProxyTest(filters = WithConfiguredBuckets.class)
public class TestS3SecurityController
{
    private final S3Client client;
    private final TestingS3SecurityController securityController;

    @Inject
    public TestS3SecurityController(S3Client client, TestingS3SecurityController securityController)
    {
        this.client = requireNonNull(client, "internalClient is null");
        this.securityController = requireNonNull(securityController, "securityFacade is null");
    }

    @AfterEach
    public void reset()
    {
        securityController.clear();
    }

    @Test
    public void testDisableListObjects()
    {
        // list buckets is allowed
        ListBucketsResponse listBucketsResponse = client.listBuckets();
        assertThat(listBucketsResponse.buckets()).extracting(Bucket::name).containsExactlyInAnyOrder("one", "two", "three");

        // list objects is currently allowed
        ListObjectsResponse listObjectsResponse = client.listObjects(r -> r.bucket("one"));
        assertThat(listObjectsResponse.contents()).isEmpty();

        // set facade that disallows list objects on bucket "one"
        securityController.setDelegate(request -> _ -> {
            if ("one".equals(request.bucketName()) && request.httpVerb().equalsIgnoreCase("get")) {
                return FAILURE;
            }
            return SUCCESS;
        });

        // list objects on "one" now disallowed
        assertThatThrownBy(() -> client.listObjects(r -> r.bucket("one")))
                .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                .extracting(S3Exception::statusCode)
                .isEqualTo(401);

        // list buckets still allowed
        listBucketsResponse = client.listBuckets();
        assertThat(listBucketsResponse.buckets()).extracting(Bucket::name).containsExactlyInAnyOrder("one", "two", "three");
    }
}
