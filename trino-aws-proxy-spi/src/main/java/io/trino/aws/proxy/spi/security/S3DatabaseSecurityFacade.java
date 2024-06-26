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

import java.util.Optional;

public interface S3DatabaseSecurityFacade
{
    S3DatabaseSecurityFacade DEFAULT = new S3DatabaseSecurityFacade() {};

    default Optional<String> tableName(Optional<String> lowercaseAction)
    {
        return Optional.empty();
    }

    default SecurityResponse nonTableOperation(Optional<String> lowercaseAction)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse tableOperation(String tableName, Optional<String> lowercaseAction)
    {
        return SecurityResponse.DEFAULT;
    }
}
