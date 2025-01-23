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
package io.trino.aws.proxy.glue.rest;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import software.amazon.awssdk.core.SdkPojo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ModelLoader
{
    private static final String REQUEST_SUFFIX = "Request";
    private static final String REQUEST_PREFIX = "AWSGlue.";

    private static final LoadedClasses loadedClasses = new LoadedClasses();

    private static class LoadedClasses
    {
        private final Map<String, Class<?>> requestClasses;
        private final Set<Class<?>> modelClasses;

        private LoadedClasses()
        {
            Map<String, Class<?>> requestClasses = new HashMap<>();
            Set<Class<?>> modelClasses = new HashSet<>();

            try {
                ClassPath.from(ClassLoader.getSystemClassLoader())
                        .getTopLevelClasses("software.amazon.awssdk.services.glue.model")
                        .stream()
                        .map(ClassPath.ClassInfo::load)
                        .filter(modelClass -> !Modifier.isAbstract(modelClass.getModifiers()))
                        .filter(SdkPojo.class::isAssignableFrom)
                        .forEach(modelClass -> {
                            modelClasses.add(modelClass);

                            if (modelClass.getSimpleName().endsWith(REQUEST_SUFFIX)) {
                                requestClasses.put(targetFromRequest(modelClass), modelClass);
                            }
                        });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            this.requestClasses = ImmutableMap.copyOf(requestClasses);
            this.modelClasses = ImmutableSet.copyOf(modelClasses);
        }
    }

    public void bindSerializers(Binder binder)
    {
        MapBinder<Class<?>, JsonDeserializer<?>> deserializerBinder = MapBinder.newMapBinder(binder, new TypeLiteral<>() {}, new TypeLiteral<>() {});
        MapBinder<Class<?>, JsonSerializer<?>> serializerBinder = MapBinder.newMapBinder(binder, new TypeLiteral<>() {}, new TypeLiteral<>() {});

        loadedClasses.modelClasses.forEach(modelClass -> {
            deserializerBinder.addBinding(modelClass).toInstance(new GlueDeserializer<>(modelClass));
            serializerBinder.addBinding(modelClass).toInstance(new GlueSerializer<>(modelClass));
        });
    }

    public Optional<Class<?>> requestClass(String target)
    {
        return Optional.ofNullable(loadedClasses.requestClasses.get(target));
    }

    private static String targetFromRequest(Class<?> requestClass)
    {
        String baseName = requestClass.getSimpleName();
        String operationName = baseName.substring(0, baseName.length() - REQUEST_SUFFIX.length());
        return REQUEST_PREFIX + operationName;
    }
}
