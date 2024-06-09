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
package io.trino.s3.proxy.spi.rest;

import jakarta.ws.rs.core.MultivaluedMap;

import java.io.InputStream;
import java.net.URI;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public record Request(
        URI requestUri,
        MultivaluedMap<String, String> requestHeaders,
        MultivaluedMap<String, String> requestQueryParameters,
        String httpVerb,
        Optional<Supplier<InputStream>> entitySupplier)
{
    public Request
    {
        requireNonNull(requestUri, "requestUri is null");
        requireNonNull(requestHeaders, "requestHeaders is null");
        requireNonNull(requestQueryParameters, "requestQueryParameters is null");
        requireNonNull(httpVerb, "httpVerb is null");
        requireNonNull(entitySupplier, "entitySupplier is null");
    }
}