package ru.ifmo.degtiarenko.splat.server;

import java.sql.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Degtjarenko Ivan on 13.10.2016.
 */
public class DBConnection {
    private final Connection connection;
    private final Lock lock;

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            Logger.getLogger(DBConnection.class.getName()).log(Level.ALL, null, e);
        }
    }

    private DBConnection(Connection connection) {
        this.connection = connection;
        lock = new ReentrantLock();
    }

    public static DBConnection createConnection(String url, String name, String password) throws Exception {
        Connection connection;
        try {
            connection = DriverManager.getConnection(url, name, password);
            connection.setAutoCommit(true);
            return new DBConnection(connection);
        } catch (SQLException e) {
            throw new Exception();
        }
    }

    public void close() throws SQLException {
        connection.close();
    }

    public AtomicLong getAmount(Integer id) throws SQLException {
        Statement statement = connection.createStatement();
        lock.lock();
        try {
            ResultSet rs = statement.executeQuery("SELECT account FROM acccounts WHERE id =" + id + ";");
            if(rs.next()) {
                return new AtomicLong(rs.getLong(1));
            } else {
                statement.executeUpdate("INSERT INTO acccounts (id, account) VALUES (" + id +", 0);");
                return new AtomicLong(0);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateData(ConcurrentMap<Integer, AtomicLong> data) throws SQLException {
        Statement statement = connection.createStatement();
        data.forEach((id, account) -> {
            try {
                statement.executeUpdate("UPDATE acccounts SET account = " + account + "WHERE id=" + id + ";");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
