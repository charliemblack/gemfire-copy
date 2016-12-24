package io.pivotal.gemfire.demo;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charlie Black on 12/23/16.
 */
public class ToolBox {
    private ToolBox() {
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

        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println("*** " + new Date());
                System.out.println("pdxRegions.keySetOnServer().size() = " + pdxRegions.keySetOnServer().size());

                clientCache.rootRegions().forEach(region -> {
                    System.out.println("Region - " + region.getName() + " size = " + region.keySetOnServer().size());
                });
            }
        }, TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(1));
    }
}
