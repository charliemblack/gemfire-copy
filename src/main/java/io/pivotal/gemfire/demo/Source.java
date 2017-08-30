package io.pivotal.gemfire.demo;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Charlie Black on 12/22/16.
 */
public class Source {
    private ClientCache clientCache;
    private String[] destinationInfo;
    private Set<RegionSource> regionSourceSet = new ConcurrentHashSet<>();

    public Source() {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                regionSourceSet.forEach(source -> {
                    try {
                        source.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }, TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10));
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
            region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
        }

        QueryService queryService = clientCache.getQueryService();
        final CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
        RegionSource regionSource = new RegionSource(openSocket(), region, regionName);
        cqAttributesFactory.addCqListener(regionSource);

        CqQuery cq = queryService.newCq("CopyCQ_" + regionName, "select * from /" + regionName + " n", cqAttributesFactory.create());
        CqResults results = cq.executeWithInitialResults();
        for (Object o : results.asList()) {
            Struct s = (Struct) o;
            Action action = new Action(s.get("key"), s.get("value"), true);
            regionSource.send(action);
        }
        regionSourceSet.add(regionSource);
        regionSource.countDownLatch.countDown();
        System.out.println("done with regionName = " + regionName);
    }

    public void setDestinationInfo(String[] destinationInfo) {
        this.destinationInfo = destinationInfo;
    }

    private Socket openSocket() throws IOException {
        Socket socket = new Socket(destinationInfo[0], Integer.parseInt(destinationInfo[1]));
        return socket;
    }

    private class RegionSource implements CqListener {

        private final ReentrantLock lock = new ReentrantLock();
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private ObjectOutputStream objectOutputStream;
        private Region region;

        public RegionSource(Socket socket, Region region, String regionName) throws IOException {
            objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            //Not possible to have another thread accessing the stream at this point since we are still in the constructor
            objectOutputStream.writeObject(regionName);
            this.region = region;
        }

        @Override
        public void onEvent(CqEvent cqEvent) {
            try {
                countDownLatch.await();
                Action action = new Action(cqEvent.getKey(), cqEvent.getNewValue());
                Operation operation = cqEvent.getBaseOperation();
                if (operation.isUpdate() || operation.isCreate()) {
                    action.setPut(true);
                    send(action);
                } else if (operation.isDestroy()) {
                    action.setPut(false);
                    send(action);
                } else {
                    System.out.println("some other event operation name =  " + operation.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onError(CqEvent cqEvent) {

        }

        @Override
        public void close() {

        }

        public void send(Action action) throws IOException {
            if (action.getValue() instanceof PdxInstance) {
                action.setPDXInstance(true);
                action.setValue(JSONFormatter.toJSON((PdxInstance) action.getValue()));
            }
            lock.lock();
            try {
                // need to protect the stream just incase the flush from the other thread has a race
                objectOutputStream.writeObject(action);
            } finally {
                lock.unlock();
            }
        }

        public void flush() throws IOException {
            lock.lock();
            try {
                // need to protect the stream just incase the flush from the other thread has a race
                objectOutputStream.flush();
            } finally {
                lock.unlock();
            }
        }
    }
}
