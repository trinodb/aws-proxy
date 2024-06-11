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
package io.trino.s3.proxy.server.testing.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class DockerAttachUtil
{
    private DockerAttachUtil() {}

    public static InputStream executeCommand(String containerId, String command)
    {
        return internalExecuteCommand(containerId, command + '\n');
    }

    public static void clearInputStreamAndClose(InputStream inputStream)
            throws IOException
    {
        try (inputStream) {
            do {
                if (inputStream.read() < 0) {
                    break;
                }
            } while (inputStream.available() > 0);
        }
    }

    @SuppressWarnings("resource")
    private static InputStream internalExecuteCommand(String containerId, String command)
    {
        CountDownLatch latch = new CountDownLatch(1);

        InputStream inputStream = new InputStream()
        {
            private int available = command.length();

            @Override
            public synchronized int read()
            {
                if (available <= 0) {
                    //latch.countDown();
                    return -1;
                }

                int result = command.charAt(command.length() - available);
                --available;

                return result;
            }

            @Override
            public synchronized int available()
            {
                return available;
            }
        };

        DockerClient dockerClient = DockerClientFactory.instance().client();

        BlockingDeque<Integer> results = new LinkedBlockingDeque<>();

        ResultCallback.Adapter<Frame> execution = dockerClient.attachContainerCmd(containerId)
                .withStdIn(inputStream)
                .withStdOut(true)
                .withStdErr(true)
                .withFollowStream(true)
                .exec(new ResultCallback.Adapter<>()
                {
                    @Override
                    public void onNext(Frame frame)
                    {
                        for (byte b : frame.getPayload()) {
                            results.addLast(b & 0xff);
                        }
                    }
                });

        return new InputStream()
        {
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public int read()
                    throws IOException
            {
                if (!closed.get()) {
                    try {
                        return results.take();
                    }
                    catch (InterruptedException _) {
                        close();
                    }
                }
                return -1;
            }

            @Override
            public int read(byte[] b, int off, int len)
                    throws IOException
            {
                int count = 0;
                while (len > 0) {
                    int i = read();
                    if (i < 0) {
                        return -1;
                    }
                    b[off++] = (byte) (i & 0xff);
                    --len;
                    ++count;

                    if (available() == 0) {
                        break;
                    }
                }

                return count;
            }

            @Override
            public int available()
            {
                return results.size();
            }

            @Override
            public void close()
                    throws IOException
            {
                if (closed.compareAndSet(false, true)) {
                    execution.close();
                }
            }
        };
    }
}
