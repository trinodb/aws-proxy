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

import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;

@FunctionalInterface
public interface S3EndpointBuilder
{
    S3EndpointBuilder STANDARD = (uriBuilder, path, bucket, region) -> {
        String host = bucket.isEmpty() ? "s3.%s.amazonaws.com".formatted(region) : "%s.s3.%s.amazonaws.com".formatted(bucket, region);
        return uriBuilder.host(host)
                .port(-1)
                .scheme("https")
                .replacePath(path)
                .build();
    };

    URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region);
}
