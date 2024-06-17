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

import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;

import static java.util.Objects.requireNonNull;

class InternalSigningContext
        implements SigningContext
{
    private final String authorization;
    private final AwsS3V4SignerParams signingParams;
    private final String signature;
    private final String dateTime;
    private final String keyPath;

    InternalSigningContext(String authorization, AwsS3V4SignerParams signingParams, String signature, String dateTime, String keyPath)
    {
        this.authorization = requireNonNull(authorization, "authorization is null");
        this.signingParams = requireNonNull(signingParams, "signingParams is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.dateTime = requireNonNull(dateTime, "dateTime is null");
        this.keyPath = requireNonNull(keyPath, "keyPath is null");
    }

    @Override
    public String authorization()
    {
        return authorization;
    }

    AwsS3V4SignerParams signingParams()
    {
        return signingParams;
    }

    String signature()
    {
        return signature;
    }

    String dateTime()
    {
        return dateTime;
    }

    String keyPath()
    {
        return keyPath;
    }
}
