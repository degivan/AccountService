package ru.ifmo.degtiarenko.splat.server;

import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Degtjarenko Ivan on 13.10.2016.
 */
public class Service implements AccountService {
    public static final String BINDING_NAME = "test/AccountService";
    private static final int CACHE_MAX_SIZE = 4_000_000;

    private final DBConnection dbConnection;
    private final Registry registry;
    private ConcurrentMap<Integer, AtomicLong> cache;

    //statistics
    private volatile int readRequestCount;
    private volatile int writeRequestCount;
    private Calendar startTime;

    public Service(int port) throws Exception {
        dbConnection = DBConnection.createConnection("jdbc:postgresql://127.0.0.1:5433/test",
                "test_user", "qwerty");
        cache = new ConcurrentHashMap<>();
        startTime = Calendar.getInstance();
        registry = LocateRegistry.createRegistry(port);
        Remote stub = UnicastRemoteObject.exportObject(this, port);
        registry.bind(BINDING_NAME, stub);
    }


    public Long getAmount(Integer id) throws RemoteException, SQLException {
        if(!cache.containsKey(id))
            cache.put(id, dbConnection.getAmount(id));
        readRequestCount++;
        return cache.get(id).longValue();
    }

    public void addAmount(Integer id, Long value) throws RemoteException, SQLException {
        AtomicLong accountBefore;
        if(!cache.containsKey(id))
            cache.putIfAbsent(id, dbConnection.getAmount(id));
        accountBefore = cache.get(id);
        cache.put(id, new AtomicLong(value + accountBefore.longValue()));
        if(cache.size() > CACHE_MAX_SIZE) {
            dbConnection.updateData(cache);
            cache.clear();
        }
        writeRequestCount++;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        label:
        while (true) {
            String command = scanner.nextLine();
            switch (command) {
                case "shutdown":
                    shutdown();
                    break label;
                case "show stat":
                    showStatistics();
                    break;
                case "reset stat":
                    resetStatistics();
                    break;
                default:
                    System.out.println("Unknown command");
                    break;
            }
        }
    }

    private void resetStatistics() {
        readRequestCount = 0;
        writeRequestCount = 0;
        startTime = Calendar.getInstance();
    }

    private void showStatistics() {
        long time = (Calendar.getInstance().getTimeInMillis() - startTime.getTimeInMillis() ) / 1000;
        System.out.println("Total amount of read requests: " + readRequestCount);
        System.out.println("Total amount of write requests: " + writeRequestCount);
        System.out.println("Average amount of read requests per second: " + readRequestCount / time);
        System.out.println("Average amount of write requests per second: " + writeRequestCount / time);
    }

    private void shutdown() {
        try {
            registry.unbind(BINDING_NAME);
            UnicastRemoteObject.unexportObject(this, true);
            dbConnection.close();
            System.out.println("Success.");
        } catch (RemoteException | SQLException | NotBoundException e) {
            System.out.println("Failed");
            System.err.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        int port = 2099;
        Service service;
        try {
            service = new Service(port);
            service.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
