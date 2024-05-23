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
package io.trino.s3.proxy.server.testing;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class ManagedS3MockContainer
{
    private final S3MockContainer container;

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForS3MockContainer {}

    @Inject
    public ManagedS3MockContainer(@ForS3MockContainer String initialBuckets)
    {
        container = new S3MockContainer("3.8.0");
        if (!initialBuckets.isEmpty()) {
            container.withInitialBuckets(initialBuckets);
        }
        container.start();
    }

    public S3MockContainer container()
    {
        return container;
    }

    @PreDestroy
    public void shutdown()
    {
        container.stop();
    }
}
