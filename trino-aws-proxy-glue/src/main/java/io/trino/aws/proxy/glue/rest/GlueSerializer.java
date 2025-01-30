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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import software.amazon.awssdk.core.SdkField;

import java.io.IOException;

class GlueSerializer<T>
        extends JsonSerializer<T>
{
    private final SerializerCommon serializerCommon;

    GlueSerializer(Class<T> requestClass)
    {
        serializerCommon = new SerializerCommon(requestClass);
    }

    @Override
    public void serialize(T value, JsonGenerator generator, SerializerProvider serializers)
            throws IOException
    {
        generator.writeStartObject();

        for (SdkField<?> sdkField : serializerCommon.sdkFields()) {
            Object fieldValue = sdkField.getValueOrDefault(value);
            if (fieldValue != null) {
                generator.writePOJOField(sdkField.memberName(), fieldValue);
            }
        }

        generator.writeEndObject();
    }
}
