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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithConfiguredBuckets;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;

import static io.airlift.http.client.HttpStatus.REQUEST_ENTITY_TOO_LARGE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

@TrinoAwsProxyTest(filters = TestMaxPayloadSize.Filter.class)
public class TestMaxPayloadSize
{
    private final S3Client s3Client;
    private final S3Client storageClient;
    private final List<String> configuredBuckets;

    public static class Filter
            extends WithConfiguredBuckets
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return super.filter(builder).withProperty("aws.proxy.request.payload.max-size", "1B");
        }
    }

    @Inject
    public TestMaxPayloadSize(S3Client s3Client, @ForS3Container S3Client storageClient, @ForS3Container List<String> configuredBuckets)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
        this.configuredBuckets = ImmutableList.copyOf(configuredBuckets);
    }

    @Test
    public void testLimitOnPut()
    {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(configuredBuckets.getFirst()).key("put").build();
        assertThatThrownBy(() -> s3Client.putObject(putObjectRequest, RequestBody.fromString("this is too big")))
                .asInstanceOf(type(S3Exception.class))
                .extracting(SdkServiceException::statusCode)
                .isEqualTo(REQUEST_ENTITY_TOO_LARGE.code());
    }

    @Test
    public void testLimitOnGet()
    {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(configuredBuckets.getFirst()).key("get").build();
        storageClient.putObject(putObjectRequest, RequestBody.fromString("this is too big for get"));

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(configuredBuckets.getFirst()).key("get").build();
        assertThatThrownBy(() -> s3Client.getObject(getObjectRequest).readAllBytes())
                .asInstanceOf(type(S3Exception.class))
                .extracting(SdkServiceException::statusCode)
                .isEqualTo(REQUEST_ENTITY_TOO_LARGE.code());
    }
}
