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

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.WebApplicationException;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.aws.proxy.spi.rest.RequestContent.ContentType.EMPTY;

public class RequestLoggerController
{
    private static final Logger log = Logger.get(RequestLoggerController.class);

    private interface LoggerProc
    {
        void log(String format, Object... args);

        boolean isEnabled();
    }

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

    private static final RequestLoggingSession NOP_REQUEST_LOGGING_SESSION = () -> {};

    private volatile LoggerProc loggerProc = debugLogger;
    private final Map<UUID, RequestLoggingSession> sessions = new ConcurrentHashMap<>();

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

    public Optional<RequestLoggingSession> currentRequestSession(UUID requestId)
    {
        return Optional.ofNullable(sessions.get(requestId));
    }

    private RequestLoggingSession internalNewRequestSession(Request request, SigningServiceType serviceType)
    {
        if (!loggerProc.isEnabled()) {
            return NOP_REQUEST_LOGGING_SESSION;
        }

        Map<String, String> entries = new ConcurrentHashMap<>();

        Map<String, String> properties = new ConcurrentHashMap<>();

        Map<String, String> errors = new ConcurrentHashMap<>();

        Map<String, Object> requestDetails = ImmutableMap.of(
                "request.id", request.requestId(),
                "request.type", serviceType,
                "request.uri", request.requestUri(),
                "request.http.method", request.httpVerb(),
                "request.http.entity", request.requestContent().contentType() != EMPTY);

        addAll(entries, requestDetails);

        logAndClear("RequestStart", entries);

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

                    logAndClear("RequestEnd", entries);
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

    private void logAndClear(String message, Map<String, String> entries)
    {
        // TODO - keep a list of recent entries, etc.

        Map<String, String> copy = ImmutableMap.copyOf(entries);
        entries.clear();

        loggerProc.log("%s: %s", message, copy);
    }
}
