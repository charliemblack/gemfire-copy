package io.pivotal.gemfire.demo;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.pdx.JSONFormatter;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * Created by Charlie Black on 12/21/16.
 */
public class Destination {

    public static final String DEFAULT_PORT = "50505";
    public static final int BULKSIZE = 100;
    public static final long TIMEOUT = 1000;
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

    public static void main(String[] args) throws TypeMismatchException, CqException, IOException, FunctionDomainException, QueryInvocationTargetException, NameResolutionException, CqExistsException {
        String locator = "localhost[10334]";
        String port = DEFAULT_PORT;
        String userName = null;
        String password = null;
        String regionPrefix = null;
        int workerthreads = 1;

        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("locator")) {
                    locator = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("destinationPort")) {
                    port = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("username")) {
                    userName = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("password")) {
                    password = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("prefix")) {
                    regionPrefix = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("workerthreads")) {
                    workerthreads = Integer.parseInt(arg.substring(arg.indexOf("=") + 1, arg.length()));
                }
            }
            Destination destination = new Destination(regionPrefix, workerthreads);
            destination.setupGemFire(ToolBox.parseLocatorInfo(locator), userName, password);
            destination.setupServerSocket(Integer.parseInt(port));
        } else {
            System.out.println("Please provide the following parameters: locator=hostname[port] <destinationPort>=50505");
        }
    }

    private void setupGemFire(String[] locatorInfo, String userName, String password) throws IOException, CqException, CqExistsException, NameResolutionException, TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {

        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/gemfire.properties"));
        ClientCacheFactory factory = new ClientCacheFactory(properties);
        factory.setPdxReadSerialized(false);
        factory.addPoolLocator(locatorInfo[0], Integer.parseInt(locatorInfo[1]));
        factory.setPoolPRSingleHopEnabled(true);
        if (userName != null && password != null) {
            factory.set("security-client-auth-init", "io.pivotal.gemfire.demo.ClientAuthentication.create");
            factory.set("security-username", userName);
            factory.set("security-password", password);
        }
        factory.set("name", "dest");
        factory.set("statistic-archive-file", "dest.gfs");

        clientCache = factory.create();
        ToolBox.addTimerForPdxTypeMetrics(clientCache);
    }

    private void setupServerSocket(int port) throws IOException {

        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket socket = serverSocket.accept();
            try {
                SocketReader socketReader = new SocketReader(socket);
                new Thread(socketReader).start();
                socketReaders.add(socketReader);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    //protocol is region|action|action|........
    private class SocketReader implements Runnable {

        private Object lock = new Object();
        private DataInputStream objectInputStream;
        private Region<Object, Object> region;
        private String regionName;
        private long lastPush = System.currentTimeMillis();
        private ConcurrentHashMap bulk = new ConcurrentHashMap();

        public SocketReader(Socket socket) throws IOException {
            objectInputStream = new DataInputStream(socket.getInputStream());
            regionName = DataSerializer.readString(objectInputStream);
            if (regionPrefix != null) {
                regionName = regionPrefix + regionName;
            }
            region = clientCache.getRegion(regionName);
            if (region == null) {
                region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
            }
        }

        @Override
        public void run() {
            try {
                System.out.println("regionName = " + regionName);
                while (true) {
                    Action action = (Action) DataSerializer.readObject(objectInputStream);
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
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void timedFlush() {
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
                Map finalTemp = temp;
                executor.execute(() -> {
                    region.putAll(finalTemp);
                });
            }
        }
    }
}
