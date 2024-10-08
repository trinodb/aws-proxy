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
package io.trino.aws.proxy.spi.security.opa;

import com.google.common.collect.ImmutableMap;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record OpaRequest(URI opaServerUri, Map<String, Object> document, MultiMap additionalHeaders)
{
    public OpaRequest
    {
        requireNonNull(opaServerUri, "opaServerUri is null");
        document = ImmutableMap.copyOf(document);
        additionalHeaders = ImmutableMultiMap.copyOf(additionalHeaders);
    }

    public OpaRequest(URI opaServerUri, Map<String, Object> document)
    {
        this(opaServerUri, document, ImmutableMultiMap.empty());
    }
}
