/*
 * Copyright 2017 Charlie Black
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.pivotal.gemfire.demo;

import com.codahale.metrics.JmxReporter;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
public class SocketSenderCacheListenerTest {


    private Executor executor = new ThreadPoolExecutor(4, 4,
            0L, MILLISECONDS,
            new LinkedBlockingQueue<>(1024));

    @Test
    public void testValid() throws Exception {

        JmxReporter.forRegistry(ToolBox.metricRegistry)
                .build().start();


        final CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            Region bulkMap = mock(Region.class);
            ClientCache clientCache = mock(ClientCache.class);
            when(clientCache.getRegion(anyString())).thenReturn(bulkMap);
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(50505);
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    countDownLatch.countDown();
                    Socket socket = serverSocket.accept();
                    new Thread(new SocketReader(socket, clientCache, "", executor)).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        countDownLatch.await();
        final String sampleJson = IOUtils.toString(SocketSenderCacheListener.class.getResourceAsStream("/json/sample.json"));
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        for (int i = 0; i < 4; i++) {

            new Thread(() -> {
                try {
                    Socket socket = new Socket("localhost", 50505);
                    SocketSenderCacheListener listener = new SocketSenderCacheListener(socket, "RegionName" + 1);
                    while (true) {
                        EntryEvent entryEvent = mock(EntryEvent.class);
                        when(entryEvent.getKey()).thenReturn("key" + atomicInteger.incrementAndGet());
                        when(entryEvent.getNewValue()).thenReturn(sampleJson);
                        listener.afterCreate(entryEvent);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        new CountDownLatch(1).await();
    }
}