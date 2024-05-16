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
package io.trino.s3.proxy.server.minio.emulation;

import com.google.common.collect.ImmutableList;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class MinioRequestImpl
        implements MinioRequest
{
    private final MultivaluedMap<String, String> headers;
    private final String httpMethod;
    private final MinioUrl url;

    MinioRequestImpl(MultivaluedMap<String, String> headers, String httpMethod, MinioUrl url)
    {
        requireNonNull(headers, "headers is null");
        this.httpMethod = requireNonNull(httpMethod, "httpMethod is null");
        this.url = requireNonNull(url, "url is null");

        this.headers = new MultivaluedHashMap<>();
        headers.forEach((key, values) -> this.headers.put(key.toLowerCase(Locale.ROOT), values));
    }

    @Override
    public Collection<String> headerNames()
    {
        return headers.keySet();
    }

    @Override
    public List<String> headerValues(String name)
    {
        return headers.getOrDefault(name, ImmutableList.of());
    }

    @Override
    public Optional<String> headerValue(String name)
    {
        return Optional.ofNullable(headers.getFirst(name));
    }

    @Override
    public String httpMethod()
    {
        return httpMethod;
    }

    @Override
    public MinioUrl url()
    {
        return url;
    }
}
