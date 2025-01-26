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

import com.google.common.collect.ImmutableSet;
import io.trino.aws.proxy.server.rest.RequestLoggerController.SaveEntry;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRequestLoggerController
{
    @Test
    public void testSavedEntries()
    {
        RequestLoggerController controller = new RequestLoggerController(new RequestLoggerConfig());
        try (RequestLoggingSession session = controller.newRequestSession(dummyRequest(), SigningServiceType.S3)) {
            session.logProperty("one", 1);
            session.logProperty("two", 2);
            session.logProperty("three", 3);
        }

        List<SaveEntry> saveEntries = controller.savedEntries(false, _ -> true);
        assertThat(saveEntries)
                .extracting(saveEntry -> saveEntry.entries().get("request.properties"))
                .containsExactly("{one=1, two=2, three=3}", null);

        saveEntries = controller.savedEntries(true, _ -> true);
        assertThat(saveEntries)
                .extracting(saveEntry -> saveEntry.entries().get("request.properties"))
                .containsExactly(null, "{one=1, two=2, three=3}");
    }

    @Test
    public void testSavedEntriesOverflow()
    {
        RequestLoggerConfig requestLoggerConfig = new RequestLoggerConfig().setRequestLoggerSavedQty(5);
        RequestLoggerController controller = new RequestLoggerController(requestLoggerConfig);
        IntStream.range(0, 10).forEach(index -> {
            try (RequestLoggingSession session = controller.newRequestSession(dummyRequest(), SigningServiceType.S3)) {
                session.logProperty("index", index);
            }
        });

        List<SaveEntry> saveEntries = controller.savedEntries(false, _ -> true);
        assertThat(saveEntries)
                .extracting(saveEntry -> saveEntry.entries().get("request.properties"))
                .containsExactly("{index=9}", null, "{index=8}", null, "{index=7}", null, "{index=6}", null, "{index=5}", null);

        saveEntries = controller.savedEntries(false, entry -> entry.entries().containsKey("request.properties"));
        assertThat(saveEntries)
                .extracting(saveEntry -> saveEntry.entries().get("request.properties"))
                .containsExactly("{index=9}", "{index=8}", "{index=7}", "{index=6}", "{index=5}");

        saveEntries = controller.savedEntries(true, entry -> entry.entries().containsKey("request.properties"));
        assertThat(saveEntries)
                .extracting(saveEntry -> saveEntry.entries().get("request.properties"))
                .containsExactly("{index=5}", "{index=6}", "{index=7}", "{index=8}", "{index=9}");
    }

    private static Request dummyRequest()
    {
        RequestAuthorization requestAuthorization = new RequestAuthorization("dummy", "us-east-1", "/", ImmutableSet.of(), "dummy", Optional.empty(), Optional.empty());
        return new Request(UUID.randomUUID(), requestAuthorization, Instant.now(), URI.create("http://dummy.com"), RequestHeaders.EMPTY, ImmutableMultiMap.empty(), "GET", RequestContent.EMPTY);
    }
}
