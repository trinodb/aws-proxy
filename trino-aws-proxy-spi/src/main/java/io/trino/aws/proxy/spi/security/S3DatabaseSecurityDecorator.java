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
package io.trino.aws.proxy.spi.security;

import io.trino.aws.proxy.spi.rest.ParsedS3Request;

import java.util.Optional;

public interface S3DatabaseSecurityDecorator
{
    default Optional<String> tableName(ParsedS3Request request, Optional<String> lowercaseAction)
    {
        return Optional.empty();
    }

    SecurityResponse tableOperation(ParsedS3Request request, String tableName, Optional<String> lowercaseAction);

    static S3SecurityFacade decorate(ParsedS3Request request, S3SecurityFacade delegate, S3DatabaseSecurityDecorator decorator)
    {
        return lowercaseAction -> decorator.tableName(request, lowercaseAction)
                .map(tableName -> decorator.tableOperation(request, tableName, lowercaseAction))
                .orElseGet(() -> delegate.apply(lowercaseAction));
    }
}
