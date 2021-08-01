package com.sanxiau.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HbaseUtils {

    public Connection getConnection() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","47.101.204.23:2181,47.101.216.12:2181,47.101.206.249:2181");

        Connection connection =
                ConnectionFactory.createConnection(config);

        return connection;
    }
}
