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
package io.trino.s3.proxy.server.testing;

import com.google.inject.BindingAnnotation;
import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.rest.TrinoS3ProxyRestConstants;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.UUID;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class TestingUtil
{
    public static final Credentials TESTING_CREDENTIALS = Credentials.build(
            new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
            new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

    // Domain name with a wildcard CNAME pointing to localhost - needed to test Virtual Host style addressing
    public static final String LOCALHOST_DOMAIN = "local.gate0.net";

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForTesting {}

    public static S3ClientBuilder clientBuilder(URI baseUrl)
    {
        URI localProxyServerUri = baseUrl.resolve(TrinoS3ProxyRestConstants.S3_PATH);

        return S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(localProxyServerUri);
    }

    private TestingUtil() {}
}
