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

import static java.util.Objects.requireNonNull;

class MinioUrlImpl
        implements MinioUrl
{
    private final String encodedPath;
    private final String encodedQuery;

    MinioUrlImpl(String encodedPath, String encodedQuery)
    {
        this.encodedPath = requireNonNull(encodedPath, "encodedPath is null");
        this.encodedQuery = requireNonNull(encodedQuery, "encodedQuery is null");
    }

    @Override
    public String encodedQuery()
    {
        return encodedQuery;
    }

    @Override
    public String encodedPath()
    {
        return encodedPath;
    }

    @Override
    public MinioUrl appendQuery(String query)
    {
        return new MinioUrlImpl(encodedPath, encodedQuery.isEmpty() ? query : (encodedPath + "&" + query));
    }
}
