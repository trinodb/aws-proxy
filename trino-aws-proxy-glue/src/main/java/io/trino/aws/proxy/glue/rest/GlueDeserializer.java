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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static software.amazon.awssdk.utils.StringUtils.capitalize;

@SuppressWarnings("unused")
class GlueDeserializer<T>
        extends JsonDeserializer<T>
{
    private final SerializerCommon serializerCommon;
    private final Map<String, Type> fieldTypeMap;

    GlueDeserializer(Class<T> requestClass)
    {
        serializerCommon = new SerializerCommon(requestClass);

        fieldTypeMap = Stream.of(requestClass.getDeclaredFields())
                .collect(toImmutableMap(field -> capitalize(field.getName()), Field::getGenericType));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(JsonParser parser, DeserializationContext context)
            throws IOException
    {
        Object builder = serializerCommon.newBuilder();

        // recommended by claude.ai
        ObjectMapper mapper = (ObjectMapper) parser.getCodec();
        JsonNode node = mapper.readTree(parser);

        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode fieldValue = node.get(fieldName);

            SdkField<?> sdkField = serializerCommon.sdkFieldsMap().get(fieldName);
            // ignore fields that are not part of the model
            if (sdkField != null) {
                Type type = fieldTypeMap.get(fieldName);
                if (type == null) {
                    return (T) context.handleUnexpectedToken(context.getContextualType(), parser);
                }

                if (type == SdkBytes.class) {
                    sdkField.set(builder, SdkBytes.fromByteArray(fieldValue.binaryValue()));
                }
                else if (type == StringColumnStatisticsData.class) {
                    StringColumnStatisticsData stringColumnStatisticsData = mapper.convertValue(fieldValue, StringColumnStatisticsData.class);
                    if (stringColumnStatisticsData.averageLength() == null) {
                        stringColumnStatisticsData = stringColumnStatisticsData.toBuilder().averageLength(0D).build();
                    }
                    sdkField.set(builder, stringColumnStatisticsData);
                }
                else {
                    JavaType javaType = context.getTypeFactory().constructType(type);
                    sdkField.set(builder, mapper.convertValue(fieldValue, javaType));
                }
            }
        }
        return (T) serializerCommon.build(builder);
    }
}
