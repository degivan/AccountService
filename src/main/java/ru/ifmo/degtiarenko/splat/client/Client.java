package ru.ifmo.degtiarenko.splat.client;

import ru.ifmo.degtiarenko.splat.config.Config;
import ru.ifmo.degtiarenko.splat.server.AccountService;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by Degtjarenko Ivan on 13.10.2016.
 */
public class Client {
    private AccountService service;

    private final Random random;
    private final int rCount;
    private final int wCount;
    private final Identifiers ids;
    private final List<Thread> threads;

    public Client(int rCount, int wCount, Identifiers ids, AccountService service) {
        this.rCount = rCount;
        this.wCount = wCount;
        this.ids = ids;
        this.service = service;

        random = new Random();
        threads = new ArrayList<>(rCount + wCount);
    }

    public static Client createClient(Config config) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry(config.getServiceHostIp(), config.getServicePort());
        AccountService service = (AccountService) registry.lookup(config.getServiceBindingName());
        return new Client(config.getClientRCount(), config.getClientWCount(), config.getClientRange(), service);
    }
    public void run() {
        for(int i = 0; i < rCount; i++) {
            threads.add(new Thread(new ReaderTask(ids)));
        }
        for(int i = 0; i < wCount; i++) {
            threads.add(new Thread(new WriterTask(ids)));
        }
        threads.forEach(Thread::start);

        Scanner scanner = new Scanner(System.in);
        while(true) {
            if(scanner.nextLine().equals("shutdown")) {
                shutdown();
                break;
            } else {
                System.out.println("Unknown command.");
            }
        }
    }

    public void shutdown() {
        threads.forEach(Thread::interrupt);
    }

    public static void main(String[] args) {
        Client client = null;
        try {
            client = Client.createClient(Config.getInstance());
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
        if (client != null) {
            client.run();
        } else {
            System.out.println("Failed to create client.");
        }
    }

    private class ReaderTask implements Runnable {
        private final Identifiers ids;

        public ReaderTask(Identifiers ids) {
            this.ids = ids;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    service.getAmount(ids.getRandomIdentifier());
                } catch (RemoteException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class WriterTask implements Runnable {
        private final Identifiers ids;

        public WriterTask(Identifiers ids) {
            this.ids = ids;
        }

        @Override
        public void run() {
            while(true) {
                try {
                    service.addAmount(ids.getRandomIdentifier(), Math.abs(random.nextLong()));
                } catch (RemoteException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
