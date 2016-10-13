package ru.ifmo.degtiarenko.splat.client;

/**
 * Created by Degtjarenko Ivan on 13.10.2016.
 */
public class ReaderTask implements Runnable {
    private final Identifiers ids;

    public ReaderTask(Identifiers ids) {
        this.ids = ids;
    }

    @Override
    public void run() {
        while(true) {

        }
    }
}
