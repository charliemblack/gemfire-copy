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
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class SocketSenderCacheListener extends CacheListenerAdapter {

    private Object writeMonitor = new Object();
    private DataOutputStream dataOutputStream;
    private Meter putMeter;
    private Meter removeMeter;
    private Histogram dataSizeHistogram;
    private boolean needToFlush = false;


    public SocketSenderCacheListener(Socket socket, String regionName) throws IOException {

        dataOutputStream = new DataOutputStream(socket.getOutputStream());
        //Not possible to have another thread accessing the stream at this point since we are still in the constructor
        DataSerializer.writeString(regionName, dataOutputStream);
        dataOutputStream.flush();
        putMeter = ToolBox.metricRegistry.meter(MetricRegistry.name("SocketSenderCacheListener", regionName, "putMeter"));
        removeMeter = ToolBox.metricRegistry.meter(MetricRegistry.name("SocketSenderCacheListener", regionName, "removeMeter"));
        dataSizeHistogram = ToolBox.metricRegistry.histogram(MetricRegistry.name("SocketSenderCacheListener", regionName, "dataSizeHistogram"));
        ToolBox.getTimer().schedule(new TimerTask() {
            @Override
            public void run() {
                if (needToFlush) {
                    try {
                        synchronized (writeMonitor) {
                            dataOutputStream.flush();
                            needToFlush = false;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, TimeUnit.SECONDS.toMillis(1));
    }

    @Override
    public void afterCreate(EntryEvent entryEvent) {
        afterUpdate(entryEvent);
    }

    @Override
    public void afterUpdate(EntryEvent entryEvent) {
        putMeter.mark();
        try {
            Action action = new Action(entryEvent.getKey(), entryEvent.getNewValue());
            action.setPut(true);
            send(action);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterInvalidate(EntryEvent entryEvent) {
        afterDestroy(entryEvent);
    }

    @Override
    public void afterDestroy(EntryEvent entryEvent) {
        removeMeter.mark();
        try {
            Action action = new Action(entryEvent.getKey(), entryEvent.getNewValue());
            action.setPut(false);
            send(action);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void send(Action action) throws IOException {
        try {
            if (action.getValue() instanceof PdxInstance) {
                action.setPDXInstance(true);
                action.setValue(JSONFormatter.toJSON((PdxInstance) action.getValue()));
            }
            write(action);
        } catch (Exception e) {
            System.out.println("Had error with " + action.getKey() + " - " + action.getValue());
        }
    }

    private void write(Action object) throws IOException {
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        DataSerializer.writeObject(object, hdos);
        dataSizeHistogram.update(hdos.size());
        synchronized (writeMonitor) {
            hdos.sendTo((DataOutput) dataOutputStream);
            needToFlush = true;
        }
    }
}
