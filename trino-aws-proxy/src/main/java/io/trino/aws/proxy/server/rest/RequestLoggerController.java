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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.WebApplicationException;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.padStart;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.aws.proxy.server.rest.RequestLoggerController.EventType.REQUEST_END;
import static io.trino.aws.proxy.server.rest.RequestLoggerController.EventType.REQUEST_START;
import static io.trino.aws.proxy.spi.rest.RequestContent.ContentType.EMPTY;
import static java.lang.Long.toHexString;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class RequestLoggerController
{
    private static final Logger log = Logger.get(RequestLoggerController.class);

    private static final AtomicLong requestCounter = new AtomicLong();

    private static final Comparator<SaveEntry> COMPARATOR = comparing(SaveEntry::entryId);
    private static final Comparator<SaveEntry> REVERSED_COMPARATOR = COMPARATOR.reversed();

    private interface LoggerProc
    {
        void log(String format, Object... args);

        boolean isEnabled();
    }

    private static final LoggerProc nopLogger = new LoggerProc()
    {
        @Override
        public void log(String format, Object... args)
        {
            // NOP
        }

        @Override
        public boolean isEnabled()
        {
            return false;
        }
    };

    private static final LoggerProc debugLogger = new LoggerProc()
    {
        @Override
        public void log(String format, Object... args)
        {
            log.debug(format, args);
        }

        @Override
        public boolean isEnabled()
        {
            return log.isDebugEnabled();
        }
    };

    private static final LoggerProc infoLogger = new LoggerProc()
    {
        @Override
        public void log(String format, Object... args)
        {
            log.info(format, args);
        }

        @Override
        public boolean isEnabled()
        {
            return log.isInfoEnabled();
        }
    };

    public record SaveEntry(String entryId, SigningServiceType serviceType, String message, Map<String, String> entries, Instant timestamp)
    {
        public SaveEntry
        {
            requireNonNull(serviceType, "serviceType is null");
            requireNonNull(entryId, "entryId is null");
            requireNonNull(message, "message is null");
            entries = ImmutableMap.copyOf(entries);
            requireNonNull(timestamp, "timestamp is null");
        }
    }

    public enum EventType
    {
        REQUEST_START,
        REQUEST_END,
    }

    public static String eventId(Instant timestamp, long requestNumber, EventType eventType)
    {
        int typeKey = switch (eventType) {
            case REQUEST_START -> 0;
            case REQUEST_END -> 1;
        };

        return "%s.%s.%s".formatted(padStart(toHexString(timestamp.toEpochMilli()), 16, '0'), padStart(toHexString(requestNumber), 16, '0'), typeKey);
    }

    private static final RequestLoggingSession NOP_REQUEST_LOGGING_SESSION = () -> {};

    private volatile LoggerProc loggerProc = nopLogger;
    private final Map<UUID, RequestLoggingSession> sessions = new ConcurrentHashMap<>();
    private final Queue<SaveEntry> saveQueue;
    private final boolean saveQueueEnabled;

    @Inject
    public RequestLoggerController(TrinoAwsProxyConfig trinoAwsProxyConfig)
    {
        // *2 because we log request/response
        saveQueue = EvictingQueue.create(trinoAwsProxyConfig.getRequestLoggerSavedQty() * 2);
        saveQueueEnabled = (trinoAwsProxyConfig.getRequestLoggerSavedQty() > 0);
    }

    @PreDestroy
    public void verifyState()
    {
        checkState(sessions.isEmpty(), "Some logging sessions were not closed: " + sessions);
    }

    // TODO - allow levels to be set for only certain users, IPs, etc.

    public void setLevelInfo()
    {
        loggerProc = infoLogger;
    }

    public void setLevelDebug()
    {
        loggerProc = debugLogger;
    }

    public RequestLoggingSession newRequestSession(Request request, SigningServiceType serviceType)
    {
        return sessions.compute(request.requestId(), (requestId, current) -> {
            checkState(current == null, "There is already a logging session for the request: " + requestId);
            return internalNewRequestSession(request, serviceType);
        });
    }

    public RequestLoggingSession currentRequestSession(UUID requestId)
    {
        return requireNonNull(sessions.get(requestId), "No RequestLoggingSession for requestId: " + requestId);
    }

    public List<SaveEntry> savedEntries(boolean startFromHead, Predicate<SaveEntry> predicate)
    {
        return saveQueue
                .stream()
                .filter(predicate)
                .sorted(startFromHead ? COMPARATOR : REVERSED_COMPARATOR)
                .collect(toImmutableList());
    }

    @VisibleForTesting
    public void clearSavedEntries()
    {
        saveQueue.clear();
    }

    private RequestLoggingSession internalNewRequestSession(Request request, SigningServiceType serviceType)
    {
        if (!loggerProc.isEnabled() && !saveQueueEnabled) {
            return NOP_REQUEST_LOGGING_SESSION;
        }

        Instant now = Instant.now();
        long requestNumber = requestCounter.getAndIncrement();

        Map<String, String> entries = new ConcurrentHashMap<>();

        Map<String, String> properties = new ConcurrentHashMap<>();

        Map<String, String> errors = new ConcurrentHashMap<>();

        Map<String, Object> requestDetails = ImmutableMap.of(
                "request.id", request.requestId(),
                "request.number", requestNumber,
                "request.timestamp", now,
                "request.type", serviceType,
                "request.uri", request.requestUri(),
                "request.http.method", request.httpVerb(),
                "request.http.entity", request.requestContent().contentType() != EMPTY);

        addAll(entries, requestDetails);

        logAndClear(requestNumber, serviceType, "RequestStart", entries, now, REQUEST_START);

        return new RequestLoggingSession()
        {
            private final Stopwatch stopwatch = Stopwatch.createStarted();
            private volatile boolean closed;

            @Override
            public void logProperty(String name, Object value)
            {
                properties.put(name, String.valueOf(value));
            }

            @Override
            public void logError(String name, Object value)
            {
                errors.put(name, String.valueOf(value));
            }

            @SuppressWarnings({"ThrowableNotThrown", "SwitchStatementWithTooFewBranches"})
            @Override
            public void logException(Throwable e)
            {
                switch (Throwables.getRootCause(e)) {
                    case WebApplicationException webApplicationException -> {
                        errors.put("webException.status", Integer.toString(webApplicationException.getResponse().getStatus()));
                        errors.put("webException.message", webApplicationException.getMessage());
                    }

                    default -> {
                        errors.put("exception.type", e.getClass().getName());
                        errors.put("exception.message", e.getMessage());
                    }
                }
            }

            @SuppressWarnings("resource")
            @Override
            public void close()
            {
                if (closed) {
                    return;
                }
                closed = true;

                try {
                    addAll(entries, requestDetails);
                    add(entries, "request.elapsed.ms", stopwatch.elapsed().toMillis());
                    add(entries, "request.properties", properties);
                    add(entries, "request.errors", errors);

                    logAndClear(requestNumber, serviceType, "RequestEnd", entries, now, REQUEST_END);
                }
                finally {
                    sessions.remove(request.requestId());
                }
            }
        };
    }

    private void addAll(Map<String, String> entries, Map<String, Object> values)
    {
        values.forEach((key, value) -> add(entries, key, value));
    }

    private void add(Map<String, String> entries, String key, Object value)
    {
        entries.put(key, String.valueOf(value));
    }

    private void logAndClear(long requestNumber, SigningServiceType serviceType, String message, Map<String, String> entries, Instant now, EventType eventType)
    {
        String eventId = eventId(now, requestNumber, eventType);

        Map<String, String> copy = ImmutableMap.<String, String>builder()
                .putAll(entries)
                .put("request.eventId", eventId)
                .build();
        entries.clear();

        loggerProc.log("%s: %s", message, copy);
        if (saveQueueEnabled) {
            saveQueue.add(new SaveEntry(eventId, serviceType, message, copy, now));
        }
    }
}
