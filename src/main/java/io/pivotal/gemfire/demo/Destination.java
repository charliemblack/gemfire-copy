package io.pivotal.gemfire.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by Charlie Black on 12/21/16.
 */

public class Destination {

    public static final String DEFAULT_PORT = "50505";
    ClientCache clientCache;
    private ObjectMapper mapper = new ObjectMapper();
    private void setupGemFire(String[] locatorInfo) throws IOException, CqException, CqExistsException, NameResolutionException, TypeMismatchException, QueryInvocationTargetException, FunctionDomainException {

        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/gemfire.properties"));
        ClientCacheFactory factory = new ClientCacheFactory(properties);
        factory.setPdxReadSerialized(false);
        factory.addPoolLocator(locatorInfo[0], Integer.parseInt(locatorInfo[1]));
        factory.setPoolPRSingleHopEnabled(true);
        factory.set("name", "dest");
        factory.set("statistic-archive-file", "dest.gfs");
        factory.setPdxReadSerialized(true);
        factory.setPdxSerializer(new ReflectionBasedAutoSerializer("com.cdk.dmg.changeset.model.*"));

        clientCache = factory.create();
        ToolBox.addTimerForPdxTypeMetrics(clientCache);
    }

    private void setupServerSocket(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket socket = serverSocket.accept();
            try {
                new Thread(new SocketReader(socket)).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    //protocol is region|action|action|........
    private class SocketReader implements Runnable {

        private ObjectInputStream objectInputStream;

        public SocketReader(Socket socket) throws IOException {
            objectInputStream = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
        }

        @Override
        public void run() {
            try {
                String regionName = (String) objectInputStream.readObject();

                Region region = clientCache.getRegion(regionName);
                if (region == null) {
                    region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
                }
                System.out.println("regionName = " + regionName);
                while (true) {
                    Action action = (Action) objectInputStream.readObject();
                    try {
                        if (action.isPut()) {
                            Object value = action.getValue();
                            if (action.isPDXInstance()) {
                                value = JSONFormatter.fromJSON((String) value);
                            } else {
                                value = mapper.readValue((String) value, action.getClassName());
                            }
                            region.put(action.getKey(), value);
                        } else {
                            region.remove(action.getKey());
                        }
                    } catch (Exception e) {
                        // to log or not to log that is the question.
                        System.out.println("WARN: Data NOT copied for the key " + action.getKey() + " from region " + regionName);
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws TypeMismatchException, CqException, IOException, FunctionDomainException, QueryInvocationTargetException, NameResolutionException, CqExistsException {
        String locator = "localhost[10334]";
        String port = DEFAULT_PORT;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("locator")) {
                    locator = arg.substring(arg.indexOf("=") + 1, arg.length());
                } else if (arg.startsWith("destinationPort")) {
                    port = arg.substring(arg.indexOf("=") + 1, arg.length());
                }
            }
            Destination destination = new Destination();
            destination.setupGemFire(ToolBox.parseLocatorInfo(locator));
            destination.setupServerSocket(Integer.parseInt(port));
        } else {
            System.out.println("Please provide the following parameters: locator=hostname[port] <destinationPort>=50505");
        }
    }
}