package com.zcunsoft.clklog.analysis.utils;

import com.clickhouse.jdbc.ClickHouseDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class ClickHouseUtil {
  private static Connection connection;

  private static final Logger logger = LoggerFactory.getLogger(ClickHouseUtil.class);

  public static Connection getConn(String host, String database, String userName, String password) {
    try {
      String url = "jdbc:clickhouse://" + host + "/" + database;
      Driver driver = new ClickHouseDriver();
      Properties prop = new Properties();
      prop.put("user", userName);
      prop.put("password", password);
      connection = driver.connect(url, prop);
      logger.info("ck conn got");
    } catch (Exception ex) {
      logger.error("ck conn error ", ex);
    }
    return connection;
  }

  public void close() throws SQLException {
    connection.close();
  }
}

