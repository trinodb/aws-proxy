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

import com.google.common.base.Splitter;
import io.trino.aws.proxy.spi.signing.ChunkSigningSession;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class AwsChunkedInputStream
        extends InputStream
{
    private final InputStream delegate;
    private final ChunkSigningSession chunkSigningSession;

    private enum State
    {
        FIRST_CHUNK,
        MIDDLE_CHUNKS,
        LAST_CHUNK,
    }

    private State state = State.FIRST_CHUNK;
    private boolean delegateIsDone;
    private int bytesRemainingInChunk;
    private int bytesAccountedFor;
    private final int decodedContentLength;

    AwsChunkedInputStream(InputStream delegate, ChunkSigningSession chunkSigningSession, int decodedContentLength)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.chunkSigningSession = requireNonNull(chunkSigningSession, "chunkSigningSession is null");
        this.decodedContentLength = decodedContentLength;
    }

    @Override
    public int read()
            throws IOException
    {
        if (isEndOfStream()) {
            return -1;
        }

        int i = delegate.read();
        if (i < 0) {
            throw new EOFException("Unexpected end of stream");
        }

        chunkSigningSession.write((byte) (i & 0xff));
        updateBytesRemaining(1);

        return i;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        if (isEndOfStream()) {
            return -1;
        }

        len = Math.min(len, bytesRemainingInChunk);

        int count = delegate.read(b, off, len);
        if (count < 0) {
            throw new EOFException("Unexpected end of stream");
        }

        chunkSigningSession.write(b, off, count);
        updateBytesRemaining(count);

        return count;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private void updateBytesRemaining(int count)
            throws IOException
    {
        bytesRemainingInChunk -= count;

        // this will read the next chunk header if we've read the entire current chunk
        // this ensures that the final chunk signature is validated before its bytes
        // are made available.
        //
        // If the final chunk signature is invalid we will not return the bytes read in
        // this read() instance and will instead throw an exception. This
        // will cause the remote to reject the request as we won't have sent all
        // the bytes specified by the Content-Length header.

        if (bytesRemainingInChunk == 0) {
            nextChunk();
        }
        else if (bytesRemainingInChunk < 0) {
            throw new IllegalStateException("bytesRemainingInChunk has gone negative: " + bytesRemainingInChunk);
        }
    }

    private boolean isEndOfStream()
            throws IOException
    {
        if (bytesRemainingInChunk == 0) {
            nextChunk();
            return bytesRemainingInChunk == 0;
        }

        return false;
    }

    private void nextChunk()
            throws IOException
    {
        if (delegateIsDone) {
            return;
        }

        switch (state) {
            case FIRST_CHUNK -> state = State.MIDDLE_CHUNKS;

            case MIDDLE_CHUNKS -> readEmptyLine();

            case LAST_CHUNK -> {
                // exit method
                return;
            }
        }

        String header = readLine();

        boolean success = false;
        do {
            List<String> parts = Splitter.on(';').trimResults().limit(2).splitToList(header);
            if (parts.size() != 2) {
                break;
            }

            int chunkSize;
            try {
                chunkSize = Integer.parseInt(parts.getFirst(), 16);
                if (chunkSize < 0) {
                    break;
                }
            }
            catch (NumberFormatException ignore) {
                break;
            }

            Optional<String> chunkSignature = Splitter.on(';').trimResults().withKeyValueSeparator('=').split(parts.get(1))
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().equalsIgnoreCase("chunk-signature"))
                    .map(Map.Entry::getValue)
                    .findFirst();

            if (chunkSignature.isEmpty()) {
                break;
            }

            chunkSigningSession.startChunk(chunkSignature.get());
            bytesRemainingInChunk = chunkSize;

            if (chunkSize == 0) {
                readEmptyLine();
                chunkSigningSession.complete();
                state = State.LAST_CHUNK;
            }
            bytesAccountedFor += chunkSize;

            success = true;
        }
        while (false);

        if (!success) {
            throw new IOException("Invalid chunk header: " + header);
        }
        if (bytesAccountedFor > decodedContentLength) {
            throw new IllegalStateException("chunked data headers report a larger size than originally declared in the request: declared %s sent %s".formatted(decodedContentLength, bytesAccountedFor));
        }
    }

    private void readEmptyLine()
            throws IOException
    {
        String crLf = readLine();
        if (!crLf.isEmpty()) {
            throw new IOException("Expected CR/LF. Instead read: " + crLf);
        }
    }

    private String readLine()
            throws IOException
    {
        StringBuilder line = new StringBuilder();
        while (true) {
            int i = delegate.read();
            if (i < 0) {
                delegateIsDone = true;
                throw new EOFException("Unexpected end of stream");
            }
            if (i == '\r') {
                break;
            }
            line.append((char) (i & 0xff));
        }

        int i = delegate.read();
        if (i != '\n') {
            throw new IOException("Expected LF. Instead read: " + i);
        }

        return line.toString();
    }
}
