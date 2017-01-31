package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrame;

import java.util.Properties;

public class JdbcInput implements BatchInput{

    public static final String JDBC_CONFIG_URL = "url";
    public static final String JDBC_CONFIG_TABLENAME = "tablename";
    public static final String JDBC_CONFIG_USERNAME = "username";
    public static final String JDBC_CONFIG_PASSWORD = "password";

    private Config config;

    @Override
    public void configure(Config config) {
        this.config = config;

        if (!config.hasPath(JDBC_CONFIG_URL)) {
            throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_URL + "' property");
        }

        if (!config.hasPath(JDBC_CONFIG_TABLENAME)) {
            throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_TABLENAME + "' property");
        }

        if (!config.hasPath(JDBC_CONFIG_USERNAME)) {
            throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_USERNAME + "' property");
        }

        if (!config.hasPath(JDBC_CONFIG_PASSWORD)) {
            throw new RuntimeException("JDBC input requires '" + JDBC_CONFIG_PASSWORD + "' property");
        }
    }

    @Override
    public DataFrame read() throws Exception {
        String url = config.getString(JDBC_CONFIG_URL);
        String tablename = config.getString(JDBC_CONFIG_TABLENAME);
        String username = config.getString(JDBC_CONFIG_USERNAME);
        String password = config.getString(JDBC_CONFIG_PASSWORD);

        Properties properties = new Properties();
        properties.put("user",username);
        properties.put("password",password);

        return Contexts.getSQLContext().read().jdbc(url,tablename,properties);
    }
}
