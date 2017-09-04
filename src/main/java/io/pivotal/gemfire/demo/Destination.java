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

import com.gemstone.gemfire.cache.client.ClientCache;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * Created by Charlie Black on 12/21/16.
 */
public class Destination {

    public static final String DEFAULT_PORT = "50505";
    private ClientCache clientCache;
    private Executor executor;
    private String regionPrefix;
    private ConcurrentLinkedQueue<SocketReader> socketReaders = new ConcurrentLinkedQueue<>();

    public Destination(String regionPrefix, int workerThreads) {
        new Timer("FlushingTimer", true).schedule(new TimerTask() {
            @Override
            public void run() {
                socketReaders.forEach(SocketReader::timedFlush);
            }
        }, TimeUnit.MILLISECONDS.toMillis(1000));

        this.regionPrefix = regionPrefix;

        executor = new ThreadPoolExecutor(workerThreads, workerThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024));
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            Properties appArgs = ToolBox.readArgs(args);

            String locator = appArgs.getProperty("locator", "localhost[10334]");
            int port = Integer.parseInt(appArgs.getProperty("destinationPort", DEFAULT_PORT));
            String userName = appArgs.getProperty("username");
            String password = appArgs.getProperty("password");
            String regionPrefix = appArgs.getProperty("prefix", "");
            int workerthreads = Integer.parseInt(appArgs.getProperty("workerthreads", "1"));

            Destination destination = new Destination(regionPrefix, workerthreads);
            destination.setClientCache(ToolBox.setupGemFire(ToolBox.parseLocatorInfo(locator), userName, password));
            destination.setupServerSocket(port);
        } else {
            System.out.println("Please provide the following parameters: locator=hostname[port] <destinationPort>=50505");
        }
    }

    private void setupServerSocket(int port) throws IOException {

        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket socket = serverSocket.accept();
            try {
                SocketReader socketReader = new SocketReader(socket, clientCache, regionPrefix, executor);
                new Thread(socketReader).start();
                socketReaders.add(socketReader);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void setClientCache(ClientCache clientCache) {
        this.clientCache = clientCache;
    }
}
