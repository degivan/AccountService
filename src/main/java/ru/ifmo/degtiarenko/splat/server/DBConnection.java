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
 * <P>A connection (session) with a specific
 * database of accounts. Every account has an identifier(or id) and balance.
 * <P>
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

    /** Creates an instance of <code>DBConnection</code>.
     * @param config configuration of <code>DBConnection</code>
     * @return new instance of <code>DBConnection</code>
     * @throws Exception if fails to connect to SQL server
     */
    public static DBConnection createConnection(Config config) throws Exception {
        Connection connection;
        try {
            connection = DriverManager.getConnection(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
            connection.setAutoCommit(true);
            checkTable(connection);
            return new DBConnection(connection);
        } catch (SQLException e) {
            throw new Exception(e);
        }
    }

    private static void checkTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS accounts (" +
                        "id INTEGER PRIMARY KEY, account BIGINT);");
    }

    /** Releases this <code>DBConnection</code> object's database and JDBC resources immediately.
     * @throws SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        connection.close();
    }

    /**
     * Gets information about account's balance with chosen <code>id</code>  from SQL server.
     * @param id identifier of an account
     * @return account's balance
     * @throws SQLException if fails to execute query
     */
    public AtomicLong getAmount(Integer id) throws SQLException {
        if(selectStatement.get() == null)
            selectStatement.set(connection.prepareStatement("SELECT account FROM accounts WHERE id = ?;"));
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
                    insertStatement.set(connection.prepareStatement("INSERT INTO accounts (id, account) VALUES (?, 0);"));
                insertStatement.get().setInt(1, id);
                insertStatement.get().executeUpdate();
                return new AtomicLong(0);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Updates balances in database.
     * @param data new balances
     * @throws SQLException  if fails to execute query
     */
    public void updateData(ConcurrentMap<Integer, AtomicLong> data) throws SQLException {
        data.forEach((id, account) -> {
            try {
                if(updateStatement.get() == null)
                    updateStatement.set(connection.prepareStatement("UPDATE accounts SET account = ? WHERE id = ?;"));
                updateStatement.get().setLong(1, account.longValue());
                updateStatement.get().setInt(2, id);
                updateStatement.get().executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
