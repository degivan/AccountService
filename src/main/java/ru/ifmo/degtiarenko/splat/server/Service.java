package ru.ifmo.degtiarenko.splat.server;

import ru.ifmo.degtiarenko.splat.config.Config;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of <code>AccountService</code> interface via RMI and PostgreSQL.
 */
public class Service implements AccountService {
    private static final int CACHE_MAX_SIZE = 4_000_000;

    private final DBConnection dbConnection;
    private final Registry registry;
    private final String bindingName;
    private ConcurrentMap<Integer, AtomicLong> cache;

    //statistics
    private AtomicInteger readRequestCount;
    private AtomicInteger writeRequestCount;
    private Calendar startTime;

    public Service(Config config) throws Exception {
        dbConnection = DBConnection.createConnection(config);
        cache = new ConcurrentHashMap<>();
        resetStatistics();
        registry = LocateRegistry.createRegistry(config.getServicePort());
        Remote stub = UnicastRemoteObject.exportObject(this, config.getServicePort());
        bindingName = config.getServiceBindingName();
        registry.bind(config.getServiceBindingName(), stub);
    }


    /**
     * Implementation of <code>AccountService</code> interface method.
     *
     * @param id balance identifier
     * @return account's balance
     * @throws RemoteException if failed to invoke method remotely
     * @throws SQLException    if failed to execute query to the database
     */
    public Long getAmount(Integer id) throws RemoteException, SQLException {
        if (!cache.containsKey(id))
            cache.put(id, dbConnection.getAmount(id));
        readRequestCount.incrementAndGet();
        return cache.get(id).longValue();
    }

    /**
     * Implementation of <code>AccountService</code> interface method.
     *
     * @param id    balance identifier
     * @param value positive or negative value, which must be added to current balance
     * @throws RemoteException if failed to invoke method remotely
     * @throws SQLException    if failed to execute query to the database
     */
    public void addAmount(Integer id, Long value) throws RemoteException, SQLException {
        AtomicLong accountBefore;
        if (!cache.containsKey(id))
            cache.putIfAbsent(id, dbConnection.getAmount(id));
        accountBefore = cache.get(id);
        cache.put(id, new AtomicLong(value + accountBefore.longValue()));
        if (cache.size() > CACHE_MAX_SIZE) {
            dbConnection.updateData(cache);
            cache.clear();
        }
        writeRequestCount.incrementAndGet();
    }

    /**
     * Starts service work.
     */
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
        readRequestCount = new AtomicInteger(0);
        writeRequestCount = new AtomicInteger(0);
        startTime = Calendar.getInstance();
    }

    private void showStatistics() {
        long time = (Calendar.getInstance().getTimeInMillis() - startTime.getTimeInMillis()) / 1000;
        System.out.println("Total amount of read requests: " + readRequestCount);
        System.out.println("Total amount of write requests: " + writeRequestCount);
        System.out.println("Average amount of read requests per second: " + readRequestCount.intValue() / time);
        System.out.println("Average amount of write requests per second: " + writeRequestCount.intValue() / time);
    }

    private void shutdown() {
        try {
            dbConnection.updateData(cache);
            registry.unbind(bindingName);
            UnicastRemoteObject.unexportObject(this, true);
            dbConnection.close();
            System.out.println("Success.");
        } catch (RemoteException | SQLException | NotBoundException e) {
            System.out.println("Failed");
            System.err.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        Service service;
        try {
            service = new Service(Config.getInstance());
            service.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
