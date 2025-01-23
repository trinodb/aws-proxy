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

import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.utils.builder.Buildable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

class SerializerCommon
{
    private final Supplier<SdkPojo> builderSupplier;
    private final List<SdkField<?>> sdkFields;
    private final Function<Object, Object> builder;

    SerializerCommon(Class<?> clazz)
    {
        requireNonNull(clazz, "clazz is null");

        try {
            Method builderMethod = clazz.getMethod("builder");
            builderSupplier = () -> {
                try {
                    return (SdkPojo) builderMethod.invoke(null);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            SdkPojo builderAndPojo = builderSupplier.get();
            sdkFields = builderAndPojo.sdkFields();

            // different AWS models have different builders. There's no common base.
            // So, find the right builder
            builder = switch (builderAndPojo) {
                case Buildable _ -> o -> ((Buildable) o).build();
                case SdkResponse.Builder _ -> o -> ((SdkResponse.Builder) o).build();
                default -> throw new RuntimeException("Unexpected builder type: " + builderAndPojo);
            };
        }
        catch (Exception e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    Object newBuilder()
    {
        return builderSupplier.get();
    }

    List<SdkField<?>> sdkFields()
    {
        return sdkFields;
    }

    Object build(Object o)
    {
        return builder.apply(o);
    }
}
