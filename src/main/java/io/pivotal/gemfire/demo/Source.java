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

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Charlie Black on 12/22/16.
 */
public class Source {
    private ClientCache clientCache;
    private String[] destinationInfo;

    private Timer timer = new Timer();

    public Source() {

    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            Properties appArgs = ToolBox.readArgs(args);

            String locator = appArgs.getProperty("locator", "localhost[10334]");
            String regions = appArgs.getProperty("regions", "");
            String destination = appArgs.getProperty("destination", "localhost[50505]");
            int workers = Integer.parseInt(appArgs.getProperty("workers", "1"));
            String userName = appArgs.getProperty("username");
            String password = appArgs.getProperty("password");

            Source source = new Source();
            source.setDestinationInfo(ToolBox.parseLocatorInfo(destination));
            source.setClientCache(ToolBox.setupGemFire(ToolBox.parseLocatorInfo(locator), userName, password));
            for (String currRegion : regions.split(",")) {
                source.setupEventListener(currRegion, workers);
            }
            //We don't want the application to finish so just wait.
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        } else {
            System.out.println("Please provide the following parameters: locator=hostname[port] regions=regionA,regionB <destination>=hostname[50505]");
        }
    }


    @SuppressWarnings("unchecked")
    private void setupEventListener(String regionName, int workers) throws CqException, CqExistsException, RegionNotFoundException, IOException {

        Region region = getRegion(regionName);
        Set<Object> keySetOnServer = region.keySetOnServer();
        System.out.println("keySetOnServer.size() = " + keySetOnServer.size());
        if (!keySetOnServer.isEmpty()) {
            int worker = 1;
            for (List<Object> keys : Iterables.partition(keySetOnServer, keySetOnServer.size() / workers)) {
                System.out.println("Starting worker " + worker + " on keys.size() " + keys.size() + " for region " + regionName);
                new Thread(new GetAllInBatches(keys, region)).start();
                worker++;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Region getRegion(String regionName) throws IOException {
        ClientRegionFactory clientRegionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
        CacheListener cacheLister = new SocketSenderCacheListener(openSocket(), regionName);
        clientRegionFactory.addCacheListener(cacheLister);
        Region region = clientRegionFactory.create(regionName);
        region.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS);
        return region;
    }

    private void setDestinationInfo(String[] destinationInfo) {
        this.destinationInfo = destinationInfo;
    }

    private Socket openSocket() throws IOException {
        return new Socket(destinationInfo[0], Integer.parseInt(destinationInfo[1]));
    }

    public void setClientCache(ClientCache clientCache) {
        this.clientCache = clientCache;
    }

    private class GetAllInBatches implements Runnable {

        private Collection keys;
        private Region region;

        GetAllInBatches(Collection keys, Region region) {
            this.keys = keys;
            this.region = region;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {

            Iterable<Collection> partitionedKeySet = Iterables.partition(keys, 100);
            int count = keys.size();
            for (Collection keySubSet : partitionedKeySet) {
                Map<Object, Object> bulk = region.getAll(keySubSet);
                count -= bulk.size();
                if (count % 1000 == 0) {
                    System.out.println(count + " items left to transfer for region " + region.getName());
                }
            }
            System.out.println(" Partition done with regionName = " + region.getName() + " key size " + keys.size());
        }
    }
}
