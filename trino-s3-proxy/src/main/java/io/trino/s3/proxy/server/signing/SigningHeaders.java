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
package io.trino.s3.proxy.server.signing;

import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.core.MultivaluedMap;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.s3.proxy.server.collections.MultiMapHelper.lowercase;
import static io.trino.s3.proxy.server.signing.SigningController.Mode.UNADJUSTED_HEADERS;
import static java.util.Objects.requireNonNull;

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

    private static final Set<String> LOWERCASE_HEADERS = ImmutableSet.of("content-type");

    private final MultivaluedMap<String, String> lowercaseHeaders;
    private final Set<String> lowercaseHeadersToSign;

    private SigningHeaders(MultivaluedMap<String, String> lowercaseHeaders, Set<String> lowercaseHeadersToSign)
    {
        this.lowercaseHeaders = requireNonNull(lowercaseHeaders, "lowercaseHeaders is null");
        this.lowercaseHeadersToSign = requireNonNull(lowercaseHeadersToSign, "lowercaseHeadersToSign is null");
    }

    static SigningHeaders build(MultivaluedMap<String, String> requestHeaders)
    {
        MultivaluedMap<String, String> lowercaseHeaders = buildLowercaseHeaders(UNADJUSTED_HEADERS, requestHeaders);

        Set<String> lowercaseHeadersToSign = new HashSet<>(lowercaseHeaders.keySet());
        lowercaseHeadersToSign.removeAll(IGNORED_HEADERS);

        return new SigningHeaders(lowercaseHeaders, ImmutableSet.copyOf(lowercaseHeadersToSign));
    }

    static SigningHeaders build(SigningController.Mode mode, MultivaluedMap<String, String> lowercaseHeaders, Set<String> headersToSign)
    {
        Set<String> lowercaseHeadersToSign = headersToSign.stream().map(header -> header.toLowerCase(Locale.ROOT)).collect(toImmutableSet());
        return new SigningHeaders(buildLowercaseHeaders(mode, lowercaseHeaders), lowercaseHeadersToSign);
    }

    boolean hasHeaderToSign(String name)
    {
        return lowercaseHeadersToSign.contains(name.toLowerCase(Locale.ROOT));
    }

    Stream<Map.Entry<String, List<String>>> lowercaseHeadersToSign()
    {
        return lowercaseHeaders.entrySet().stream()
                .filter(entry -> lowercaseHeadersToSign.contains(entry.getKey()));
    }

    Optional<String> getFirst(String lowercaseHeader)
    {
        return Optional.ofNullable(lowercaseHeaders.getFirst(lowercaseHeader));
    }

    private static MultivaluedMap<String, String> buildLowercaseHeaders(SigningController.Mode mode, MultivaluedMap<String, String> headers)
    {
        BiFunction<String, List<String>, List<String>> lowercaseHeaderValues = (key, values) -> {
            // mode == UNADJUSTED_HEADERS is temp until airlift is fixed
            if ((mode == UNADJUSTED_HEADERS) || !LOWERCASE_HEADERS.contains(key)) {
                return values;
            }
            return values.stream().map(value -> value.toLowerCase(Locale.ROOT)).collect(toImmutableList());
        };

        return lowercase(headers, lowercaseHeaderValues);
    }
}
