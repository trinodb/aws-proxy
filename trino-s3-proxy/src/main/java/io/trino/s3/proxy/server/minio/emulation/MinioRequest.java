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

import jakarta.ws.rs.core.MultivaluedMap;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface MinioRequest
{
    Collection<String> headerNames();

    List<String> headerValues(String name);

    Optional<String> headerValue(String name);

    String httpMethod();

    MinioUrl url();

    static MinioRequest build(MultivaluedMap<String, String> headers, String httpMethod, MinioUrl url)
    {
        return new MinioRequestImpl(headers, httpMethod, url);
    }
}
