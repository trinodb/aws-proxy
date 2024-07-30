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

public interface S3SecurityFacade
{
    S3SecurityFacade NOOP = _ -> SecurityResponse.SUCCESS;

    String UPLOADS_ACTION = "uploads";
    String DELETE_ACTION = "delete";

    /**
     *
     * Actions are S3 operations such as CreateSession, PutBucketAcl, etc.
     * see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html">API Operations</a> for details.
     * The "Request Syntax" for each API lists the action as the first query parameter
     * E.g. <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketIntelligentTieringConfiguration.html">DeleteBucketIntelligentTieringConfiguration</a>
     * {@code /?intelligent-tiering&id=Id} - the action is {@code intelligent-tiering}.
     */
    SecurityResponse apply(Optional<String> lowercaseAction);
}
