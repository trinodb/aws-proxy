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

import jakarta.ws.rs.core.MultivaluedMap;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.s3.proxy.server.collections.MultiMapHelper.lowercase;
import static java.util.Objects.requireNonNull;

public record ParsedS3Request(
        String bucketName,
        String keyInBucket,
        MultivaluedMap<String, String> requestHeaders,
        MultivaluedMap<String, String> requestQueryParameters,
        String httpVerb,
        Optional<Supplier<InputStream>> entitySupplier)
{
    public ParsedS3Request
    {
        requireNonNull(bucketName, "bucketName is null");
        requireNonNull(keyInBucket, "keyInBucket is null");
        requestHeaders = lowercase(requireNonNull(requestHeaders, "requestHeaders is null"));
        requireNonNull(requestQueryParameters, "requestQueryParameters is null");
        requireNonNull(httpVerb, "httpVerb is null");
        requireNonNull(entitySupplier, "entitySupplier is null");
    }
}
