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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;

import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charlie Black on 12/23/16.
 */
public class ToolBox {
    public static final MetricRegistry metricRegistry = new MetricRegistry();

    private static Timer timer = new Timer();

    private ToolBox() {
    }

    public static Timer getTimer() {
        return timer;
    }

    public static String[] parseLocatorInfo(String locatorString) {
        String[] locatorInfo = new String[2];
        int i = locatorString.indexOf("[");
        if (i >= 0) {
            locatorInfo[0] = locatorString.substring(0, i);
        }
        int j = locatorString.indexOf("]");
        if (j >= 0) {
            locatorInfo[1] = locatorString.substring(i + 1, j);
        }
        return locatorInfo;
    }

    public static void addTimerForPdxTypeMetrics(final ClientCache clientCache) {

        Region temp = clientCache.getRegion("PdxTypes");
        if (temp == null) {
            temp = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("PdxTypes");
        }
        final Region pdxRegions = temp;

        metricRegistry.register(MetricRegistry.name("PdxTypes", "count"),
                (Gauge<Integer>) () -> pdxRegions.keySetOnServer().size());

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);
    }

    public static Properties readArgs(String[] args) {
        Properties properties = new Properties(System.getProperties());
        for (String curr : args) {
            if (curr.contains("=")) {
                String[] keyAndValue = curr.split("=");
                if (keyAndValue.length == 2) {
                    properties.setProperty(keyAndValue[0], keyAndValue[1]);
                }
            }
        }
        return properties;
    }

    public static ClientCache setupGemFire(String[] locatorInfo, String userName, String password) throws Exception {
        Properties properties = new Properties();
        properties.load(ToolBox.class.getResourceAsStream("/gemfire.properties"));
        ClientCacheFactory factory = new ClientCacheFactory(properties);
        factory.setPoolSubscriptionEnabled(true);
        factory.addPoolLocator(locatorInfo[0], Integer.parseInt(locatorInfo[1]));
        factory.setPoolPRSingleHopEnabled(true);
        factory.setPoolMaxConnections(-1);

        System.out.println("userName = " + userName);
        System.out.println("password = " + password);
        if (userName != null && password != null) {
            factory.set("security-client-auth-init", "io.pivotal.gemfire.demo.ClientAuthentication.create");
            factory.set("security-username", userName);
            factory.set("security-password", password);
        }
        factory.set("name", "source");
        factory.set("statistic-archive-file", "source.gfs");
        ClientCache clientCache = factory.create();
        ToolBox.addTimerForPdxTypeMetrics(clientCache);
        return clientCache;
    }
}
