package io.pivotal.gemfire.demo;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class CheckData {
    public static void main(String[] args) throws IOException {
        String locator = "localhost[10334]";
        String regions = "";
        String userName = null;
        String password = null;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("locator")) {
                    locator = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("regions")) {
                    regions = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("username")) {
                    userName = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("password")) {
                    password = arg.substring(arg.indexOf("=") + 1, arg.length());
                }
            }
            Properties properties = new Properties();

            properties.load(CheckData.class.getResourceAsStream("/gemfire.properties"));
            ClientCacheFactory factory = new ClientCacheFactory();
            factory.setPoolSubscriptionEnabled(true);
            String[] locatorInfo = ToolBox.parseLocatorInfo(locator);

            factory.addPoolLocator(locatorInfo[0], Integer.parseInt(locatorInfo[1]));
            if (userName != null && password != null) {
                factory.set("security-client-auth-init", "io.pivotal.gemfire.demo.ClientAuthentication.create");
                factory.set("security-username", userName);
                factory.set("security-password", password);
            }
            factory.set("name", "checkdata");
            factory.set("statistic-archive-file", "checkdata.gfs");
            ClientCache clientCache = factory.create();
            System.out.println("clientCache = " + clientCache);
            System.out.println("regions = " + Arrays.toString(regions.split(",")));
            for (String currRegion : regions.split(",")) {
                Region region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(currRegion);
                System.out.println("region.getName() = " + region.getName());
                System.out.println("region.keySetOnServer().size() = " + region.keySetOnServer().size());
                
                for (Object key : region.keySetOnServer()) {
                    try {
                        Object value = region.get(key);
                    } catch (Exception e) {
                        System.err.println("e = " + e);
                        System.out.println("problem with key = " + key);
                    }
                }
                System.out.println(" done region.getName() = " + region.getName());
            }

        } else {
            System.out.println("Please provide the following parameters: locator=hostname[port] regions=regionA,regionB ");
        }

        System.exit(0);
    }
}
