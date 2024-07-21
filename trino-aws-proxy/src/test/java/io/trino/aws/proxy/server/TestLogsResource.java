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
package io.trino.aws.proxy.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.rest.RequestLoggerController.EventType;
import io.trino.aws.proxy.server.rest.RequestLoggingSession;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

import java.net.URI;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.aws.proxy.server.rest.RequestLoggerController.EventType.REQUEST_END;
import static io.trino.aws.proxy.server.rest.RequestLoggerController.EventType.REQUEST_START;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest
public class TestLogsResource
{
    private static final SigningServiceType FAKE_SERVICE = new SigningServiceType("test");

    private final RequestLoggerController loggerController;
    private final ObjectMapper objectMapper;
    private final CloudWatchLogsClient cloudWatchClient;

    @Inject
    public TestLogsResource(TestingHttpServer httpServer, RequestLoggerController loggerController, TrinoAwsProxyConfig config, @ForTesting Credentials testingCredentials, ObjectMapper objectMapper)
    {
        this.loggerController = requireNonNull(loggerController, "loggerController is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");

        URI uri = UriBuilder.fromUri(httpServer.getBaseUrl()).path(config.getLogsPath()).build();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey());

        cloudWatchClient = CloudWatchLogsClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(uri)
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }

    @BeforeEach
    public void reset()
    {
        loggerController.clearSavedEntries();
    }

    @Test
    public void testInitialState()
    {
        GetLogEventsRequest request = GetLogEventsRequest.builder().build();
        GetLogEventsResponse response = cloudWatchClient.getLogEvents(request);
        assertThat(response.events()).isEmpty();

        loggerController.clearSavedEntries();

        // log endpoint requests are not included by default - specifically ask for them
        request = GetLogEventsRequest.builder().logStreamName("logs").build();
        response = cloudWatchClient.getLogEvents(request);
        // the request start for this getLogEvents call()
        assertThat(response.events()).hasSize(1);
    }

    @Test
    public void testOrdering()
    {
        // We expect to see 2 entries per log, one for request processing start and one for request processing end
        final int logQty = 500;

        Supplier<GetLogEventsRequest.Builder> builderProc = () -> GetLogEventsRequest.builder().logStreamName(FAKE_SERVICE.serviceName()).startFromHead(false).limit(logQty);

        GetLogEventsResponse initialResponse = cloudWatchClient.getLogEvents(builderProc.get().build());
        assertThat(initialResponse.events()).isEmpty();

        addFakeRequestLogs(logQty);

        GetLogEventsResponse firstBackwardsBatchResponse = cloudWatchClient.getLogEvents(builderProc.get().build());

        assertThat(firstBackwardsBatchResponse.events()).hasSize(logQty);
        long lastRequestNumberInFirstBatch = assertOrdering(firstBackwardsBatchResponse.events(), false);
        String backwardToken = firstBackwardsBatchResponse.nextBackwardToken();

        GetLogEventsResponse secondBackwardsBatchResponse = cloudWatchClient.getLogEvents(builderProc.get().nextToken(backwardToken).build());
        assertThat(secondBackwardsBatchResponse.events()).hasSize(logQty);
        long lastRequestNumberInSecondBatch = assertOrdering(secondBackwardsBatchResponse.events(), false);
        assertThat(lastRequestNumberInSecondBatch).isLessThan(lastRequestNumberInFirstBatch);
        // AWS considers pagination to be done when the next token equals the token provided
        assertThat(secondBackwardsBatchResponse.nextBackwardToken()).isEqualTo(backwardToken);

        GetLogEventsResponse forwardBatch = cloudWatchClient.getLogEvents(builderProc.get().startFromHead(true).nextToken(secondBackwardsBatchResponse.nextForwardToken()).build());
        assertThat(forwardBatch.events()).hasSize(logQty);
        assertOrdering(forwardBatch.events(), true);

        /*
        There are 2 batches in total, with 500 logs each:

        0.......................499.......................999
        <..secondBackwardsBatch..> <..firstBackwardsBatch..>

        firstBackwardsBatch and forwardBatch (which we retrieve by iterating forwards from secondBackwardsBatch) should be identical
         */
        assertThat(forwardBatch.events().stream().map(this::parseEvent)).containsExactlyInAnyOrderElementsOf(firstBackwardsBatchResponse.events().stream().map(this::parseEvent).toList());
    }

    @Test
    public void testPagingForwardsPagesSameSize()
    {
        testPaging(10, 50, true);
    }

    @Test
    public void testPagingForwardsPagesDifferentSizes()
    {
        testPaging(14, 73, true);
    }

    @Test
    public void testPagingForwardsPageLargerThanLogQty()
    {
        testPaging(100, 10, true);
    }

    @Test
    public void testPagingBackwardsPagesSameSize()
    {
        testPaging(10, 50, false);
    }

    @Test
    public void testPagingBackwardPagesDifferentSizes()
    {
        testPaging(14, 73, false);
    }

    @Test
    public void testPagingBackwardPageLargerThanLogQty()
    {
        testPaging(100, 10, false);
    }

    private void testPaging(int pageSize, int logsToAdd, boolean isAscending)
    {
        final int allExpectedLogs = logsToAdd * 2;
        final int expectedBatches = Math.ceilDiv(allExpectedLogs, pageSize);
        final int lastBatchSize = allExpectedLogs % pageSize == 0 ? pageSize : allExpectedLogs % pageSize;

        addFakeRequestLogs(logsToAdd);

        Iterator<List<OutputLogEvent>> eventsIter = getBatchedLogs(Optional.empty(), isAscending, pageSize);
        for (int i = 1; i <= expectedBatches; i++) {
            assertThat(eventsIter.hasNext()).isTrue();
            List<OutputLogEvent> nextBatch = eventsIter.next();
            if (i < expectedBatches) {
                assertThat(nextBatch.size()).isEqualTo(pageSize);
            }
            else {
                assertThat(nextBatch.size()).isEqualTo(lastBatchSize);
            }
            assertOrdering(nextBatch, isAscending);
        }
        if (expectedBatches > 1) {
            // We can only determine when we are on the last batch based on the next markers that we get back
            // from our requests, so we cannot keep this assertion in cases where a single batch is expected
            assertThat(eventsIter.hasNext()).isFalse();
        }
    }

    private long assertOrdering(List<OutputLogEvent> events, boolean expectAscending)
    {
        // This method expects the start/end processing log to be included
        // When using paging, you should use even page sizes
        Event lastChecked = null;
        List<Event> orderedEvents = (expectAscending ? events.reversed() : events).stream().map(this::parseEvent).collect(toImmutableList());
        for (int i = 1; i < orderedEvents.size(); i += 2) {
            Event previous = orderedEvents.get(i - 1);
            Event check = orderedEvents.get(i);
            assertThat(check).extracting(Event::requestNumber, Event::eventType).containsExactly(previous.requestNumber, REQUEST_START);
            assertThat(previous.eventType).isEqualTo(REQUEST_END);
            assertThat(check.requestNumber).isEqualTo(previous.requestNumber);

            lastChecked = check;
        }
        assertThat(lastChecked).isNotNull();
        return lastChecked.requestNumber;
    }

    private Iterator<List<OutputLogEvent>> getBatchedLogs(Optional<String> nextToken, boolean startFromHead, int limit)
    {
        return new LogsIterator(cloudWatchClient, nextToken, startFromHead, limit);
    }

    private static class LogsIterator
            implements Iterator<List<OutputLogEvent>>
    {
        private boolean hasNext = true;
        private final CloudWatchLogsClient client;
        private Optional<String> nextToken;
        private final boolean startFromHead;
        private final int limit;

        private LogsIterator(CloudWatchLogsClient client, Optional<String> nextToken, boolean startFromHead, int limit)
        {
            this.client = requireNonNull(client, "client is null");
            this.nextToken = requireNonNull(nextToken, "nextToken is null");
            this.startFromHead = startFromHead;
            this.limit = limit;
        }

        @Override
        public boolean hasNext()
        {
            return hasNext;
        }

        @Override
        public List<OutputLogEvent> next()
        {
            GetLogEventsRequest.Builder requestBuilder = GetLogEventsRequest.builder().logStreamName(FAKE_SERVICE.serviceName()).limit(limit).startFromHead(startFromHead);
            nextToken.ifPresent(requestBuilder::nextToken);
            GetLogEventsResponse response = client.getLogEvents(requestBuilder.build());
            String subsequentToken = startFromHead ? response.nextForwardToken() : response.nextBackwardToken();
            if (nextToken.isPresent()) {
                String nextTokenValue = nextToken.get();
                hasNext = !(nextTokenValue.equals(subsequentToken));
            }
            nextToken = Optional.of(subsequentToken);
            return response.events();
        }
    }

    private void addFakeRequestLogs(int qty)
    {
        while (qty-- > 0) {
            RequestAuthorization fakeRequestAuthorization = new RequestAuthorization("DUMMY-ACCESS-KEY", "us-east-1", "/hey", ImmutableSet.of(), "dummy", Optional.empty(), Optional.empty());
            Request fakeRequest = new Request(UUID.randomUUID(), fakeRequestAuthorization, Instant.now(), URI.create("http://foo.bar"), RequestHeaders.EMPTY, ImmutableMultiMap.empty(), "GET", RequestContent.EMPTY);
            try (RequestLoggingSession session = loggerController.newRequestSession(fakeRequest, FAKE_SERVICE)) {
                session.logProperty("foo", "bar");
            }
        }
    }

    private record Event(long epoch, long requestNumber, EventType eventType)
    {
        private Event
        {
            requireNonNull(eventType, "eventType is null");
        }
    }

    private Event fromSpec(String spec)
    {
        List<String> parts = Splitter.on('.').splitToList(spec);
        assertThat(parts).hasSize(3);

        long epoch = Long.parseLong(parts.getFirst(), 16);
        long requestNumber = Long.parseLong(parts.get(1), 16);
        EventType eventType = switch (parts.getLast()) {
            case "0" -> REQUEST_START;
            case "1" -> EventType.REQUEST_END;
            default -> Assertions.fail("Unknown event type: " + parts.getLast());
        };

        return new Event(epoch, requestNumber, eventType);
    }

    @SuppressWarnings("unchecked")
    private Event parseEvent(OutputLogEvent event)
    {
        Map<String, String> properties = (Map<String, String>) deserialize(event).get("properties");
        return fromSpec(properties.get("request.eventId"));
    }

    private Map<String, Object> deserialize(OutputLogEvent event)
    {
        try {
            return objectMapper.readValue(event.message(), new TypeReference<>() {});
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
