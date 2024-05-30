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
package io.trino.s3.proxy.server.rest;

import io.airlift.http.client.BodyGenerator;

import java.io.InputStream;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

class StreamingBodyGenerator
        implements BodyGenerator
{
    private final InputStream source;

    StreamingBodyGenerator(InputStream source)
    {
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public void write(OutputStream out)
            throws Exception
    {
        source.transferTo(out);
        out.flush();
    }
}
