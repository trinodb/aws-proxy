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
package io.trino.aws.proxy.spi.signing;

import io.trino.aws.proxy.spi.collections.MultiMap;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.rest.Request;

import java.net.URI;
import java.time.Instant;
import java.util.function.Function;

public interface SigningController
{
    String formatRequestInstant(Instant instant);

    String formatResponseInstant(Instant instant);

    String signRequest(
            SigningMetadata metadata,
            String region,
            String requestDate,
            Function<Credentials, Credential> credentialsSupplier,
            URI requestURI,
            MultiMap requestHeaders,
            MultiMap queryParameters,
            String httpMethod);

    SigningMetadata validateAndParseAuthorization(Request request, SigningServiceType signingServiceType);
}
