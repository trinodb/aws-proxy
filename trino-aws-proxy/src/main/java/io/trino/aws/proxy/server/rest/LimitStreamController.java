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

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import jakarta.ws.rs.WebApplicationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static jakarta.ws.rs.core.Response.Status.REQUEST_ENTITY_TOO_LARGE;

public class LimitStreamController
{
    private final Optional<DataSize> quota;

    @Inject
    public LimitStreamController(TrinoAwsProxyConfig trinoAwsProxyConfig)
    {
        quota = trinoAwsProxyConfig.getMaxPayloadSize();
    }

    public InputStream wrap(InputStream inputStream)
    {
        return quota.map(q -> internalWrap(inputStream, q.toBytes())).orElse(inputStream);
    }

    private static InputStream internalWrap(InputStream inputStream, long quota)
    {
        CountingInputStream delegate = new CountingInputStream(inputStream);
        return new InputStream()
        {
            @Override
            public int read()
                    throws IOException
            {
                return validate(delegate.read());
            }

            @Override
            public int read(byte[] b, int off, int len)
                    throws IOException
            {
                return validate(delegate.read(b, off, len));
            }

            @Override
            public long skip(long n)
                    throws IOException
            {
                return validate(delegate.skip(n));
            }

            @Override
            public void mark(int readlimit)
            {
                delegate.mark(readlimit);
                validate();
            }

            @Override
            public void reset()
                    throws IOException
            {
                delegate.reset();
                validate();
            }

            @Override
            public boolean markSupported()
            {
                return validate(delegate.markSupported());
            }

            @Override
            public void close()
                    throws IOException
            {
                delegate.close();
            }

            private void validate()
            {
                validate(null);
            }

            private <T> T validate(T value)
            {
                if (delegate.getCount() > quota) {
                    throw new WebApplicationException(REQUEST_ENTITY_TOO_LARGE);
                }
                return value;
            }
        };
    }

    public OutputStream wrap(OutputStream outputStream)
    {
        return quota.map(q -> internalWrap(outputStream, q.toBytes())).orElse(outputStream);
    }

    private OutputStream internalWrap(OutputStream outputStream, long quota)
    {
        CountingOutputStream delegate = new CountingOutputStream(outputStream);

        return new OutputStream()
        {
            @Override
            public void write(byte[] b)
                    throws IOException
            {
                delegate.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len)
                    throws IOException
            {
                delegate.write(b, off, len);
                validate();
            }

            @Override
            public void flush()
                    throws IOException
            {
                delegate.flush();
            }

            @Override
            public void close()
                    throws IOException
            {
                delegate.close();
            }

            @Override
            public void write(int b)
                    throws IOException
            {
                delegate.write(b);
                validate();
            }

            private void validate()
            {
                if (delegate.getCount() > quota) {
                    throw new WebApplicationException(REQUEST_ENTITY_TOO_LARGE);
                }
            }
        };
    }
}
