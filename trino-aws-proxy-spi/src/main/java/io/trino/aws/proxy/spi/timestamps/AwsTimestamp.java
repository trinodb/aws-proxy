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
package io.trino.aws.proxy.spi.timestamps;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class AwsTimestamp
{
    public static final ZoneId ZONE = ZoneId.of("Z");
    private static final DateTimeFormatter AMZ_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.US).withZone(ZONE);
    private static final DateTimeFormatter RESPONSE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'", Locale.US).withZone(ZONE);

    public static String toRequestFormat(Instant instant)
    {
        return AMZ_DATE_FORMAT.format(instant);
    }

    public static String toResponseFormat(Instant instant)
    {
        return RESPONSE_DATE_FORMAT.format(instant);
    }

    public static Instant fromRequestTimestamp(String requestTimestamp)
    {
        return ZonedDateTime.parse(requestTimestamp, AMZ_DATE_FORMAT).toInstant();
    }

    private AwsTimestamp() {}
}
