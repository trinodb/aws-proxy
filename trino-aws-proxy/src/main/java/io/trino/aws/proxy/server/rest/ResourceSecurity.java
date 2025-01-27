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

import io.trino.aws.proxy.spi.signing.SigningServiceType;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({TYPE, METHOD})
public @interface ResourceSecurity
{
    sealed interface AccessType
    {
    }

    sealed interface PublicAccessType
            extends AccessType
            permits Public
    {}

    non-sealed interface SigV4AccessType
            extends AccessType
    {
        SigningServiceType signingServiceType();
    }

    final class Public
            implements PublicAccessType
    {}

    final class S3
            implements SigV4AccessType
    {
        @Override
        public SigningServiceType signingServiceType()
        {
            return SigningServiceType.S3;
        }
    }

    final class Sts
            implements SigV4AccessType
    {
        @Override
        public SigningServiceType signingServiceType()
        {
            return SigningServiceType.STS;
        }
    }

    final class Logs
            implements SigV4AccessType
    {
        @Override
        public SigningServiceType signingServiceType()
        {
            return SigningServiceType.LOGS;
        }
    }

    Class<? extends AccessType> value();
}
