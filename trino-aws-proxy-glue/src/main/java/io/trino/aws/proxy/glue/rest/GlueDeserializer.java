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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import software.amazon.awssdk.core.SdkField;

import java.io.IOException;
import java.util.Map;

class GlueDeserializer<T>
        extends JsonDeserializer<T>
{
    private final SerializerCommon serializerCommon;

    GlueDeserializer(Class<T> requestClass)
    {
        serializerCommon = new SerializerCommon(requestClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(JsonParser parser, DeserializationContext context)
            throws IOException
    {
        Object builder = serializerCommon.newBuilder();

        Map<String, Object> value = parser.readValueAs(new TypeReference<>() {});
        for (SdkField<?> sdkField : serializerCommon.sdkFields()) {
            sdkField.set(builder, value.get(sdkField.memberName()));
        }
        return (T) serializerCommon.build(builder);
    }
}
