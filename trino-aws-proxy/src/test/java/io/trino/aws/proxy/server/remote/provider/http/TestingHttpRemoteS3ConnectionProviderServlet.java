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
package io.trino.aws.proxy.server.remote.provider.http;

import com.google.common.collect.Multimap;
import io.airlift.http.client.HttpStatus;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableListMultimap.flatteningToImmutableListMultimap;
import static java.util.Arrays.stream;

public final class TestingHttpRemoteS3ConnectionProviderServlet
        extends HttpServlet
{
    private final List<Multimap<String, String>> requestParameters = new ArrayList<>();

    private Optional<String> response = Optional.empty();
    private Optional<HttpStatus> responseStatus = Optional.empty();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException
    {
        requestParameters.add(req.getParameterMap().entrySet().stream().collect(
                flatteningToImmutableListMultimap(Map.Entry::getKey, value -> stream(value.getValue()))));

        if (!req.getPathInfo().equals("/api/v1/remote_s3_connection")) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found");
            return;
        }

        if (responseStatus.isPresent()) {
            HttpStatus actualResponseStatus = responseStatus.orElseThrow();
            resp.sendError(actualResponseStatus.code(), actualResponseStatus.reason());
            return;
        }

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        resp.getWriter().write(response.orElseThrow());
    }

    public void setResponse(String response)
    {
        this.response = Optional.of(response);
    }

    public void setResponseStatusOverride(HttpStatus status)
    {
        this.responseStatus = Optional.of(status);
    }

    public void reset()
    {
        requestParameters.clear();
        response = Optional.empty();
        responseStatus = Optional.empty();
    }

    public List<Multimap<String, String>> getRequestParameters()
    {
        return requestParameters;
    }
}
