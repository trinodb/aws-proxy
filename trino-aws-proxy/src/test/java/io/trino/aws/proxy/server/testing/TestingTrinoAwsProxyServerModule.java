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
package io.trino.aws.proxy.server.testing;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.trino.aws.proxy.server.TrinoAwsProxyServerModule;
import io.trino.aws.proxy.server.rest.S3PresignController;
import io.trino.aws.proxy.server.security.S3SecurityController;

public class TestingTrinoAwsProxyServerModule
        extends TrinoAwsProxyServerModule
{
    @Override
    protected void installS3SecurityController(Binder binder)
    {
        binder.bind(S3SecurityController.class).to(TestingS3SecurityController.class).in(Scopes.SINGLETON);
        binder.bind(TestingS3SecurityController.class).in(Scopes.SINGLETON);

        binder.bind(S3PresignController.class).to(TestingS3PresignController.class).in(Scopes.SINGLETON);
        binder.bind(TestingS3PresignController.class).in(Scopes.SINGLETON);
    }
}
