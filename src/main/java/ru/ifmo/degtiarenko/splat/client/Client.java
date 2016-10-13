package ru.ifmo.degtiarenko.splat.client;

import ru.ifmo.degtiarenko.splat.server.AccountService;
import ru.ifmo.degtiarenko.splat.server.Service;

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
    private static final String HOST_IP = "localhost";
    private static final int PORT = 2099;

    private AccountService service;

    private final Random random;
    private final int rCount;
    private final int wCount;
    private final Identifiers ids;
    private final List<Thread> threads;

    public Client(int rCount, int wCount, Identifiers ids) throws RemoteException, NotBoundException {
        this.rCount = rCount;
        this.wCount = wCount;
        this.ids = ids;

        random = new Random();
        threads = new ArrayList<>(rCount + wCount);

        Registry registry = LocateRegistry.getRegistry(HOST_IP, PORT);
        service = (AccountService) registry.lookup(Service.BINDING_NAME);
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
            client = new Client(Integer.parseInt(args[0]), Integer.parseInt(args[1]), new Identifiers(args[2]));
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
        if (client != null) {
            client.run();
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

    public class WriterTask implements Runnable {
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
