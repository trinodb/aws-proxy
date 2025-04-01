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
package io.trino.aws.proxy.server.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.json.JsonCodec.jsonCodec;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

public class TestingHttpCredentialsProviderServlet
        extends HttpServlet
{
    public static final String DUMMY_EMULATED_ACCESS_KEY = "test-emulated-access-key";
    public static final String DUMMY_EMULATED_SECRET_KEY = "test-emulated-secret-key";

    private final Map<String, String> expectedHeaders;
    private final AtomicInteger requestCounter;

    public TestingHttpCredentialsProviderServlet(Map<String, String> expectedHeaders)
    {
        this.expectedHeaders = ImmutableMap.copyOf(expectedHeaders);
        this.requestCounter = new AtomicInteger();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        requestCounter.addAndGet(1);
        for (Map.Entry<String, String> expectedHeader : expectedHeaders.entrySet()) {
            if (!expectedHeader.getValue().equals(request.getHeader(expectedHeader.getKey()))) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
        }

        Optional<String> sessionToken = Optional.ofNullable(request.getParameter("sessionToken"));
        String emulatedAccessKey = request.getPathInfo().substring(1);
        // The session token in the request is legal if it is either:
        // - Not present
        // - Matching our test logic: it should be equal to the access-key + "-token"
        boolean isLegalSessionToken = sessionToken
                .map(presentSessionToken -> "%s-token".formatted(emulatedAccessKey).equals(presentSessionToken))
                .orElse(true);
        if (!isLegalSessionToken) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        switch (emulatedAccessKey) {
            case DUMMY_EMULATED_ACCESS_KEY -> {
                Credential emulated = new Credential(DUMMY_EMULATED_ACCESS_KEY, DUMMY_EMULATED_SECRET_KEY, sessionToken);
//                Credential remote = new Credential(DUMMY_REMOTE_ACCESS_KEY, DUMMY_REMOTE_SECRET_KEY);
                IdentityCredential credentials = new IdentityCredential(emulated, new TestingIdentity("test-username", ImmutableList.of(), "xyzpdq"));
                String jsonCredentials = jsonCodec(IdentityCredential.class).toJson(credentials);
                response.setContentType(APPLICATION_JSON);
                response.getWriter().print(jsonCredentials);
            }
            case "incorrect-response" -> {
                response.getWriter().print("incorrect response");
            }
            default -> response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    public int getRequestCount()
    {
        return requestCounter.get();
    }

    public void resetRequestCount()
    {
        requestCounter.set(0);
    }
}
