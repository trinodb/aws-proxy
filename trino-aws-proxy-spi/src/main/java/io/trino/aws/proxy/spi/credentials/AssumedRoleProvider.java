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
package io.trino.aws.proxy.spi.credentials;

import java.util.Optional;

public interface AssumedRoleProvider
{
    AssumedRoleProvider NOOP = (_, _, _, _, _, _) -> Optional.empty();

    /**
     * Assume a role if possible. The details of role assuming are implementation
     * specific. Your implementation should have a centralized role assuming
     * mechanism, likely some type of database along with a way of registering
     * roles assuming policies, etc.
     */
    Optional<EmulatedAssumedRole> assumeEmulatedRole(
            Credential emulatedCredential,
            String region,
            String requestArn,
            Optional<String> requestExternalId,
            Optional<String> requestRoleSessionName,
            Optional<Integer> requestDurationSeconds);
}
