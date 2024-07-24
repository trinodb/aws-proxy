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

import static java.util.Objects.requireNonNull;

public sealed interface SecurityResponse
{
    SecurityResponse SUCCESS = new Success();
    SecurityResponse FAILURE = new Failure();

    record Success()
            implements SecurityResponse
    {
    }

    record Failure(Optional<String> error)
            implements SecurityResponse
    {
        public Failure
        {
            requireNonNull(error, "error is null");
        }

        public Failure()
        {
            this(Optional.empty());
        }

        public Failure(String error)
        {
            this(Optional.of(error));
        }
    }
}
