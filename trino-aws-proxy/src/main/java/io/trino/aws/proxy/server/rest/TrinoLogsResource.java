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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.rest.RequestLoggerController.SaveEntry;
import io.trino.aws.proxy.server.rest.ResourceSecurity.Logs;
import io.trino.aws.proxy.server.rest.TrinoLogsResource.GetLogEventsResponse.Event;
import io.trino.aws.proxy.spi.rest.Request;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.aws.proxy.spi.signing.SigningServiceType.S3;
import static io.trino.aws.proxy.spi.signing.SigningServiceType.STS;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.util.Objects.requireNonNull;

@ResourceSecurity(Logs.class)
public class TrinoLogsResource
{
    private static final Set<String> DEFAULT_STREAMS = ImmutableSet.of(S3.serviceName(), STS.serviceName());

    private enum TokenType
    {
        BACKWARDS("b/"),
        FORWARDS("f/");

        private final String prefix;

        private Optional<String> atIndex(int index)
        {
            return Optional.of(prefix + index);
        }

        TokenType(String prefix)
        {
            this.prefix = requireNonNull(prefix, "prefix is null");
        }
    }

    private final RequestLoggerController loggerController;
    private final ObjectMapper objectMapper;

    // see https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_GetLogEvents.html
    public record GetLogEventsRequest(
            Optional<Long> startTime,
            Optional<Long> endTime,
            Optional<Integer> limit,
            Optional<String> nextToken,
            Optional<Boolean> startFromHead,
            Optional<Set<String>> logStreamNames,
            Optional<String> logStreamName)
    {
        public GetLogEventsRequest
        {
            requireNonNull(startTime, "startTime is null");
            requireNonNull(endTime, "endTime is null");
            requireNonNull(limit, "limit is null");
            requireNonNull(nextToken, "nextToken is null");
            requireNonNull(startFromHead, "startFromHead is null");
            requireNonNull(logStreamNames, "logStreamNames is null");
            requireNonNull(logStreamName, "logStreamName is null");

            if (logStreamNames.isEmpty() && logStreamName.isEmpty()) {
                logStreamNames = Optional.of(DEFAULT_STREAMS);
            }
        }
    }

    // see https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_GetLogEvents.html
    public record GetLogEventsResponse(List<Event> events, Optional<String> nextBackwardToken, Optional<String> nextForwardToken)
    {
        public GetLogEventsResponse
        {
            events = ImmutableList.copyOf(events);
            requireNonNull(nextBackwardToken, "nextBackwardToken is null");
            requireNonNull(nextForwardToken, "nextForwardToken is null");
        }

        public record Event(String logStreamName, String eventId, long ingestionTime, long timestamp, String message)
        {
            public Event
            {
                requireNonNull(logStreamName, "logStreamName is null");
                requireNonNull(eventId, "eventId is null");
                requireNonNull(message, "message is null");
            }
        }
    }

    @Inject
    public TrinoLogsResource(RequestLoggerController loggerController, ObjectMapper objectMapper)
    {
        this.loggerController = requireNonNull(loggerController, "loggerController is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response processRequest(@Context Request request, @HeaderParam("X-Amz-Target") String target)
    {
        String command = Splitter.on('.').trimResults().limit(2).splitToList(target).getLast().toLowerCase(Locale.ROOT);
        return switch (command) {
            // filter/get log events are basically the same.
            // We've made GetLogEventsRequest a union of both and one method can satisfy both
            case "filterlogevents", "getlogevents" -> getLogEvents(request);
            default -> Response.status(BAD_REQUEST).build();
        };
    }

    private Response getLogEvents(Request request)
    {
        byte[] bytes = request.requestContent().standardBytes().orElseThrow(() -> new WebApplicationException(BAD_REQUEST));
        GetLogEventsRequest getLogEvents;
        try {
            getLogEvents = objectMapper.readValue(bytes, GetLogEventsRequest.class);
        }
        catch (IOException e) {
            throw new WebApplicationException(e, BAD_REQUEST);
        }

        // TODO - support filter patterns

        boolean startFromHead = getLogEvents.startFromHead.orElse(false);
        Predicate<SaveEntry> predicate = entry -> startTimePasses(getLogEvents, entry) &&
                endTimePasses(getLogEvents, entry) &&
                logStreamNamesPasses(entry, getLogEvents);
        int limit = getLogEvents.limit.orElse(Integer.MAX_VALUE);

        List<SaveEntry> filteredEntries = loggerController.savedEntries(startFromHead, predicate);

        record StartIndex(TokenType tokenType, int index) {}

        // next token is merely an index
        StartIndex startIndex = getLogEvents.nextToken().map(nextToken -> {
            try {
                TokenType tokenType;
                if (nextToken.startsWith(TokenType.FORWARDS.prefix)) {
                    tokenType = TokenType.FORWARDS;
                }
                else if (nextToken.startsWith(TokenType.BACKWARDS.prefix)) {
                    tokenType = TokenType.BACKWARDS;
                }
                else {
                    throw new WebApplicationException("Invalid nextToken", BAD_REQUEST);
                }
                return new StartIndex(tokenType, Integer.parseInt(nextToken.substring(2)));
            }
            catch (NumberFormatException _) {
                throw new WebApplicationException("Invalid nextToken", BAD_REQUEST);
            }
        }).orElseGet(() -> startFromHead ? new StartIndex(TokenType.FORWARDS, 0) : new StartIndex(TokenType.BACKWARDS, Math.max(filteredEntries.size() - 1, 0)));

        // Track the number of items we need to skip depending on the token we received
        // and the order of iteration
        int positionInFilteredStream;
        if (startFromHead) {
            positionInFilteredStream = startIndex.index;
        }
        else {
            // The AWS spec mandates startFromHead must be true if using a forward token
            if (startIndex.tokenType == TokenType.FORWARDS) {
                throw new WebApplicationException("Invalid startIndex", BAD_REQUEST);
            }
            // If traversing backwards, the index in the reversed stream needs to be computed based on its size
            positionInFilteredStream = Math.max(filteredEntries.size() - startIndex.index - 1, 0);
        }
        List<Event> events = filteredEntries.stream()
                .skip(positionInFilteredStream)
                .limit(limit)
                .map(entry -> new Event("trino", entry.entryId(), entry.timestamp().toEpochMilli(), entry.timestamp().toEpochMilli(), format(entry)))
                .collect(toImmutableList());

        Optional<String> nextBackwardToken;
        Optional<String> nextForwardToken;
        if (startFromHead) {
            nextForwardToken = ((startIndex.index + limit) < filteredEntries.size()) ? TokenType.FORWARDS.atIndex(startIndex.index + limit) : TokenType.FORWARDS.atIndex(startIndex.index);
            nextBackwardToken = (startIndex.index - 1 >= 0) ? TokenType.BACKWARDS.atIndex(startIndex.index - 1) : TokenType.BACKWARDS.atIndex(startIndex.index);
        }
        else {
            nextBackwardToken = ((startIndex.index - limit) >= 0) ? TokenType.BACKWARDS.atIndex(startIndex.index - limit) : TokenType.BACKWARDS.atIndex(startIndex.index);
            nextForwardToken = (startIndex.index + 1) < filteredEntries.size() ? TokenType.FORWARDS.atIndex(startIndex.index + 1) : TokenType.FORWARDS.atIndex(startIndex.index);
        }
        GetLogEventsResponse response = new GetLogEventsResponse(events, nextBackwardToken, nextForwardToken);

        return Response.ok(response).build();
    }

    private static boolean logStreamNamesPasses(SaveEntry entry, GetLogEventsRequest getLogEvents)
    {
        return getLogEvents.logStreamName.map(logStreamName -> logStreamNamePasses(logStreamName, entry))
                .orElseGet(() -> getLogEvents.logStreamNames
                        .map(logStreamNames -> logStreamNames.stream().anyMatch(logStreamName -> logStreamNamePasses(logStreamName, entry)))
                        .orElse(true));
    }

    private static boolean logStreamNamePasses(String logStreamName, SaveEntry entry)
    {
        return entry.serviceType().serviceName().equalsIgnoreCase(logStreamName);
    }

    private static boolean startTimePasses(GetLogEventsRequest getLogEvents, SaveEntry entry)
    {
        long timestamp = entry.timestamp().toEpochMilli();
        return getLogEvents.startTime
                .map(startTime -> startTime <= timestamp)
                .orElse(true);
    }

    private static boolean endTimePasses(GetLogEventsRequest getLogEvents, SaveEntry entry)
    {
        long timestamp = entry.timestamp().toEpochMilli();
        return getLogEvents.endTime
                .map(endTime -> endTime > timestamp)
                .orElse(true);
    }

    private String format(SaveEntry entry)
    {
        ObjectNode messageNode = objectMapper.createObjectNode();
        messageNode.put("message", entry.message());

        ObjectNode entriesNode = objectMapper.createObjectNode();
        entry.entries().forEach(entriesNode::put);
        messageNode.putPOJO("properties", entriesNode);

        return messageNode.toString();
    }
}
