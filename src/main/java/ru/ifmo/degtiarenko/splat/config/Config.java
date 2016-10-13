package ru.ifmo.degtiarenko.splat.config;

import ru.ifmo.degtiarenko.splat.client.BadArgumentException;
import ru.ifmo.degtiarenko.splat.client.Identifiers;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Configuration of server, database connection and client.
 */
public class Config {
    private static Config INSTANCE = null;
    private final int servicePort;
    private final String serviceHostIp;
    private final String serviceBindingName;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final int clientRCount;
    private final int clientWCount;
    private final Identifiers clientRange;

    /**
     * Gets <code>Config</code> instance created from config.xml
     *
     * @return instance of <code>Config</code>
     */
    public static Config getInstance() {
        if (INSTANCE == null)
            try {
                INSTANCE = new Config();
            } catch (IOException e) {
                System.out.println("Error: cannot read config.xml.");
                System.err.println(e.toString());
                System.exit(0);
            } catch (BadArgumentException e) {
                System.out.println("Error: client.range value is not in acceptable format");
                System.err.println(e.toString());
                System.exit(0);
            }
        return INSTANCE;
    }

    private Config() throws IOException, BadArgumentException {
        Properties properties = new Properties();
        properties.loadFromXML(new FileInputStream("config.xml"));

        servicePort = Integer.parseInt(properties.getProperty("service.port"));
        serviceHostIp = properties.getProperty("service.host_ip");
        serviceBindingName = properties.getProperty("service.binding_name");
        jdbcUrl = properties.getProperty("jdbc.url");
        jdbcUser = properties.getProperty("jdbc.user");
        jdbcPassword = properties.getProperty("jdbc.pass");
        clientRCount = Integer.parseInt(properties.getProperty("client.rcount"));
        clientWCount = Integer.parseInt(properties.getProperty("client.wcount"));
        clientRange = new Identifiers(properties.getProperty("client.range"));
    }

    public int getServicePort() {
        return servicePort;
    }

    public String getServiceHostIp() {
        return serviceHostIp;
    }

    public String getServiceBindingName() {
        return serviceBindingName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public int getClientRCount() {
        return clientRCount;
    }

    public int getClientWCount() {
        return clientWCount;
    }

    public Identifiers getClientRange() {
        return clientRange;
    }
}
