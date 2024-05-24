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

import io.trino.s3.proxy.server.rest.S3EndpointBuilder;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class TestingS3EndpointBuilder
        implements S3EndpointBuilder
{
    private final AtomicReference<S3EndpointBuilder> delegate = new AtomicReference<>(S3EndpointBuilder.STANDARD);

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        return delegate.get().buildEndpoint(uriBuilder, path, bucket, region);
    }

    public void setDelegate(S3EndpointBuilder delegate)
    {
        this.delegate.set(requireNonNull(delegate, "delegate is null"));
    }
}
