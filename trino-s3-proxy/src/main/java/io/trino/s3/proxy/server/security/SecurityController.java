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
package io.trino.s3.proxy.server.security;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.trino.s3.proxy.server.rest.ParsedS3Request;

import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SecurityController
{
    private final SecurityFacadeProvider securityFacadeProvider;

    @Inject
    public SecurityController(SecurityFacadeProvider securityFacadeProvider)
    {
        this.securityFacadeProvider = requireNonNull(securityFacadeProvider, "securityFacadeProvider is null");
    }

    public SecurityResponse apply(ParsedS3Request request)
    {
        SecurityFacade securityFacade = securityFacadeProvider.securityFacadeForRequest(request);

        Optional<String> lowercaseAction = request.rawQuery().flatMap(SecurityController::parseAction);

        return securityFacade.apply(lowercaseAction);
    }

    private static Optional<String> parseAction(String rawQuery)
    {
        if (rawQuery.isBlank()) {
            return Optional.empty();
        }

        String possibleAction = Splitter.on('&').limit(1).splitToList(rawQuery).getFirst();
        boolean isAction = !possibleAction.contains("=");
        return isAction ? Optional.of(possibleAction.toLowerCase(Locale.ROOT)) : Optional.empty();
    }
}
