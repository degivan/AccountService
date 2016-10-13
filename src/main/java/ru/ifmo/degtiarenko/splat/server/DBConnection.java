package ru.ifmo.degtiarenko.splat.server;

import ru.ifmo.degtiarenko.splat.config.Config;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Degtjarenko Ivan on 13.10.2016.
 */
public class DBConnection implements AutoCloseable {
    private final Connection connection;
    private final ThreadLocal<PreparedStatement> updateStatement;
    private final ThreadLocal<PreparedStatement> insertStatement;
    private final ThreadLocal<PreparedStatement> selectStatement;
    private final ConcurrentMap<Integer, Lock> locks;

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            Logger.getLogger(DBConnection.class.getName()).log(Level.ALL, null, e);
        }
    }

    private DBConnection(Connection connection) throws SQLException {
        this.connection = connection;
        locks = new ConcurrentHashMap<>();
        updateStatement = new ThreadLocal<>();
        insertStatement = new ThreadLocal<>();
        selectStatement = new ThreadLocal<>();
    }

    public static DBConnection createConnection(Config config) throws Exception {
        Connection connection;
        try {
            connection = DriverManager.getConnection(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
            connection.setAutoCommit(true);
            return new DBConnection(connection);
        } catch (SQLException e) {
            throw new Exception(e);
        }
    }

    public void close() throws SQLException {
        connection.close();
    }

    public AtomicLong getAmount(Integer id) throws SQLException {
        if(selectStatement.get() == null)
            selectStatement.set(connection.prepareStatement("SELECT account FROM acccounts WHERE id = ?;"));
        selectStatement.get().setInt(1, id);

        Lock lock = locks.getOrDefault(id, null);
        if (lock == null)
        {
            locks.putIfAbsent(id, new ReentrantLock());
            lock = locks.get(id);
        }
        lock.lock();
        try {
            ResultSet rs = selectStatement.get().executeQuery();
            if(rs.next()) {
                return new AtomicLong(rs.getLong(1));
            } else {
                if(insertStatement.get() == null)
                    insertStatement.set(connection.prepareStatement("INSERT INTO acccounts (id, account) VALUES (?, 0);"));
                insertStatement.get().setInt(1, id);
                insertStatement.get().executeUpdate();
                return new AtomicLong(0);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateData(ConcurrentMap<Integer, AtomicLong> data) throws SQLException {
        data.forEach((id, account) -> {
            try {
                if(updateStatement.get() == null)
                    updateStatement.set(connection.prepareStatement("UPDATE acccounts SET account = ? WHERE id = ?;"));
                updateStatement.get().setInt(1, id);
                updateStatement.get().setLong(2, account.longValue());
                updateStatement.get().executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
