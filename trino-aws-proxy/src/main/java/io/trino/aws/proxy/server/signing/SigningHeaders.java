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
package io.trino.aws.proxy.server.signing;

import com.google.common.collect.ImmutableSet;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import io.trino.aws.proxy.spi.util.MultiMap;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

final class SigningHeaders
{
    private static final Set<String> IGNORED_HEADERS = ImmutableSet.of(
            "x-amz-decoded-content-length",
            "x-amzn-trace-id",
            "expect",
            "accept-encoding",
            "authorization",
            "user-agent",
            "connection");

    private final MultiMap headers;
    private final Set<String> lowercaseHeadersToSign;

    private SigningHeaders(MultiMap headers, Set<String> lowercaseHeadersToSign)
    {
        this.headers = ImmutableMultiMap.copyOf(headers);
        this.lowercaseHeadersToSign = ImmutableSet.copyOf(lowercaseHeadersToSign);
    }

    static SigningHeaders build(MultiMap requestHeaders)
    {
        Set<String> lowercaseHeadersToSign = new HashSet<>(requestHeaders.keySet());
        lowercaseHeadersToSign.removeAll(IGNORED_HEADERS);

        return new SigningHeaders(requestHeaders, ImmutableSet.copyOf(lowercaseHeadersToSign));
    }

    static SigningHeaders build(MultiMap requestHeaders, Set<String> lowercaseHeadersToSign)
    {
        return new SigningHeaders(requestHeaders, lowercaseHeadersToSign);
    }

    boolean hasHeaderToSign(String name)
    {
        return lowercaseHeadersToSign.contains(name.toLowerCase(Locale.ROOT));
    }

    Stream<Map.Entry<String, List<String>>> lowercaseHeadersToSign()
    {
        return headers.entrySet().stream()
                .filter(entry -> lowercaseHeadersToSign.contains(entry.getKey()));
    }

    Optional<String> getFirst(String lowercaseHeader)
    {
        return headers.getFirst(lowercaseHeader);
    }
}
