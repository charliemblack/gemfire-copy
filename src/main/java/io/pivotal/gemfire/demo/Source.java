package io.pivotal.gemfire.demo;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.google.common.collect.Iterables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charlie Black on 12/22/16.
 */
public class Source {
    private ClientCache clientCache;
    private String[] destinationInfo;
    private Set<MyCacheListener> regionSourceSet = new ConcurrentHashSet<>();

    public Source() {
    }

    public static void main(String[] args) throws TypeMismatchException, CqException, IOException, FunctionDomainException, QueryInvocationTargetException, NameResolutionException, CqExistsException {
        String locator = "localhost[10334]";
        String regions = "";
        String destination = "localhost[50505]";
        String userName = null;
        String password = null;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("locator")) {
                    locator = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("regions")) {
                    regions = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("destination")) {
                    destination = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("username")) {
                    userName = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("password")) {
                    password = arg.substring(arg.indexOf("=") + 1, arg.length());
                }
            }
            Source source = new Source();
            source.setDestinationInfo(ToolBox.parseLocatorInfo(destination));
            source.setupGemFire(ToolBox.parseLocatorInfo(locator), userName, password);
            for (String currRegion : regions.split(",")) {
                source.setUpCQOnRegion(currRegion);
            }
        } else {
            System.out.println("Please provide the following parameters: locator=hostname[port] regions=regionA,regionB <destination>=hostname[50505]");
        }
    }

    private void setupGemFire(String[] locatorInfo, String userName, String password) throws IOException, CqException, CqExistsException, NameResolutionException, TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/gemfire.properties"));
        ClientCacheFactory factory = new ClientCacheFactory(properties);

        factory.setPoolSubscriptionEnabled(true);
        factory.addPoolLocator(locatorInfo[0], Integer.parseInt(locatorInfo[1]));

        if (userName != null && password != null) {
            factory.set("security-client-auth-init", "io.pivotal.gemfire.demo.ClientAuthentication.create");
            factory.set("security-username", userName);
            factory.set("security-password", password);
        }
        factory.set("name", "source");
        factory.set("statistic-archive-file", "source.gfs");
        clientCache = factory.create();
        ToolBox.addTimerForPdxTypeMetrics(clientCache);
    }


    private void setUpCQOnRegion(String regionName) throws CqException, CqExistsException, RegionNotFoundException, IOException {

        Region region = clientCache.getRegion(regionName);

        if (region == null) {
            ClientRegionFactory clientRegionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
            region = clientRegionFactory.create(regionName);
        }


        MyCacheListener regionSource = new MyCacheListener(openSocket(), region, regionName);
        region.getAttributesMutator().addCacheListener(regionSource);
        region.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS);
        Collection keySetOnServer = region.keySetOnServer();
        System.out.println("keySetOnServer.size() = " + keySetOnServer.size());

        Iterable<Collection> partitionedKeySet = Iterables.partition(keySetOnServer, 100);
        for (Collection keySubSet : partitionedKeySet) {
            Map<Object, Object> bulk = region.getAll(keySubSet);
        }
        regionSourceSet.add(regionSource);
        System.out.println("done with regionName = " + regionName);
    }

    public void setDestinationInfo(String[] destinationInfo) {
        this.destinationInfo = destinationInfo;
    }

    private Socket openSocket() throws IOException {
        Socket socket = new Socket(destinationInfo[0], Integer.parseInt(destinationInfo[1]));
        return socket;
    }

    private class MyCacheListener extends CacheListenerAdapter {

        private DataOutputStream objectOutputStream;
        private Region region;

        public MyCacheListener(Socket socket, Region region, String regionName) throws IOException {

            objectOutputStream = new DataOutputStream(socket.getOutputStream());
            //Not possible to have another thread accessing the stream at this point since we are still in the constructor
            objectOutputStream.writeUTF(regionName);
            objectOutputStream.flush();
            this.region = region;
        }

        @Override
        public void afterCreate(EntryEvent entryEvent) {
            afterUpdate(entryEvent);
        }

        @Override
        public void afterUpdate(EntryEvent entryEvent) {
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
            try {
                Action action = new Action(entryEvent.getKey(), entryEvent.getNewValue());
                action.setPut(false);
                send(action);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void send(Action action) throws IOException {
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

        public void write(Object object) throws IOException {
            HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
            DataSerializer.writeObject(object, hdos);
            byte[] buffer = hdos.toByteArray();
            objectOutputStream.writeInt(buffer.length);
            objectOutputStream.write(buffer);
        }
    }
}
