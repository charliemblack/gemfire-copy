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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.pdx.JSONFormatter;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

//protocol is region|action|action|........
public class SocketReader implements Runnable {
    public static final int BULKSIZE = 100;
    public static final long TIMEOUT = 1000;
    private final Executor executor;
    private final Socket socket;
    private Object lock = new Object();
    private DataInputStream objectInputStream;
    private Map<Object, Object> region;
    private String regionName;
    private long lastPush = System.currentTimeMillis();
    private ConcurrentHashMap bulk = new ConcurrentHashMap();
    private Histogram bulkSizeHistogram;
    private Meter socketReadRate;


    public SocketReader(Socket socket, ClientCache clientCache, String regionPrefix, Executor executor) throws IOException {
        this.socket = socket;
        objectInputStream = new DataInputStream(socket.getInputStream());
        regionName = DataSerializer.readString(objectInputStream);
        if (regionPrefix != null) {
            regionName = regionPrefix + regionName;
        }
        region = clientCache.getRegion(regionName);
        if (region == null) {
            region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
        }
        this.executor = executor;
        String name = MetricRegistry.name("SocketReader", regionName, "bulkSizeHistogram");
        bulkSizeHistogram = ToolBox.metricRegistry.histogram(name);
        socketReadRate = ToolBox.metricRegistry.meter(MetricRegistry.name("SocketReader", regionName, "socketReadRate"));
    }

    @Override
    public void run() {
        try {
            while (true) {
                Action action = DataSerializer.readObject(objectInputStream);
                socketReadRate.mark();
                try {
                    if (action.isPut()) {
                        Object value = action.getValue();
                        if (action.isPDXInstance()) {
                            try {
                                value = JSONFormatter.fromJSON((String) value);
                            } catch (Exception e) {
                                // to log or not to log that is the question.
                                e.printStackTrace();
                                value = null;
                            }
                        }
                        if (value != null) {
                            put(action.getKey(), value);
                        }
                    } else {
                        bulk.remove(action.getKey());
                        region.remove(action.getKey());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("data that caused issue = " + action);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.out.println("Reader closing socket " + socket);
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
                System.out.println("Exception while closing the socket.   Should be fine.");
            }
        }
    }

    public void timedFlush() {
        if (lastPush + TIMEOUT < System.currentTimeMillis()) {
            doBulkPut();
        }
    }

    private void put(Object key, Object value) {
        bulk.put(key, value);
        if (bulk.size() >= BULKSIZE) {
            doBulkPut();
        }
    }

    private void doBulkPut() {
        Map temp = null;
        lastPush = System.currentTimeMillis();
        synchronized (lock) {
            if (!bulk.isEmpty()) {
                temp = bulk;
                bulk = new ConcurrentHashMap();
            }
        }
        if (temp != null) {
            bulkSizeHistogram.update(temp.size());
            Map finalTemp = temp;
            executor.execute(() -> {
                region.putAll(finalTemp);
            });
        }
    }
}