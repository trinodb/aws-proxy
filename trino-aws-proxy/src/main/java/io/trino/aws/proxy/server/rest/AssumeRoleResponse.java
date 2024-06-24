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
package io.trino.aws.proxy.server.rest;

import static java.util.Objects.requireNonNull;

public record AssumeRoleResponse(AssumeRoleResult assumeRoleResult)
{
    public record AssumeRoleResult(AssumedRoleUser assumedRoleUser, Credentials credentials)
    {
        public AssumeRoleResult
        {
            requireNonNull(assumedRoleUser, "assumedRoleUser is null");
            requireNonNull(credentials, "credentials is null");
        }
    }

    public AssumeRoleResponse
    {
        requireNonNull(assumeRoleResult, "assumeRoleResult is null");
    }

    public record AssumedRoleUser(String arn, String assumedRoleId)
    {
        public AssumedRoleUser
        {
            requireNonNull(arn, "arn is null");
            requireNonNull(assumedRoleId, "assumedRoleId is null");
        }
    }

    public record Credentials(String accessKeyId, String secretAccessKey, String sessionToken, String expiration)
    {
        public Credentials
        {
            requireNonNull(accessKeyId, "accessKeyId is null");
            requireNonNull(secretAccessKey, "secretAccessKey is null");
            requireNonNull(sessionToken, "sessionToken is null");
            requireNonNull(expiration, "expiration is null");
        }
    }
}
