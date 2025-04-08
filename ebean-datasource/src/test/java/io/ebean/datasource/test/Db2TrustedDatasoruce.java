package io.ebean.datasource.test;

import com.ibm.db2.jcc.DB2Connection;
import com.ibm.db2.jcc.DB2PooledConnection;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author Roland Praml, Foconis Analytics GmbH
 */
public class Db2TrustedDatasoruce implements DataSource {

  private ConnectionInfo info;
  private String user;
  private String password;

  public Db2TrustedDatasoruce(String url, String user, String password) throws SQLException {
    try {
      this.info = ConnectionInfo.parse(url);
      this.user = user;
      this.password = password;
    } catch (URISyntaxException e) {
      throw new SQLException("invalid url: " + url, e);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {

    com.ibm.db2.jcc.DB2ConnectionPoolDataSource ds1 =
      new com.ibm.db2.jcc.DB2ConnectionPoolDataSource();
    ds1.setServerName(info.host);
    ds1.setPortNumber(info.port);
    ds1.setDatabaseName(info.dbName);
    ds1.setDriverType(4);
    ds1.setUser(user);
    ds1.setPassword(password);

    Object[] objects = ds1.getDB2TrustedPooledConnection(user, password, info.properties);
    DB2PooledConnection pooledCon = (DB2PooledConnection) objects[0];
    byte[] cookie = (byte[]) objects[1];

    return new Db2TrustedConnection((DB2Connection) pooledCon.getConnection(), cookie, user);
  }


  /**
   * Helper, that parses the JDBC-URL like jdbc:db2trusted://localhost:40005/migtest:currentSchema=METRICSTASK2;
   * in host/port/db (similar! to DB2 syntax)
   */
  private static class ConnectionInfo {
    final String host;
    final int port;
    final String dbName;
    final Properties properties;


    public ConnectionInfo(String host, int port, String dbName, Properties properties) {
      this.host = host;
      this.port = port;
      this.dbName = dbName;
      this.properties = properties;
    }

    static ConnectionInfo parse(String url) throws URISyntaxException {
      assert url.startsWith("jdbc:");
      URI uri = new URI(url.substring(5));

      String host = uri.getHost();
      int port = uri.getPort();
      if (port == 0) {
        port = 50000;
      }

      String path = uri.getPath();
      if (path.startsWith("/")) {
        path = path.substring(1);
      }
      int colon = path.indexOf(':');
      String dbName = colon == -1 ? path : path.substring(0, colon);


      Properties properties = new Properties();
      if (colon != -1) {
        String propertiesString = path.substring(colon + 1);

        String[] keyValuePairs = propertiesString.split(";");
        for (String pair : keyValuePairs) {
          String[] keyValue = pair.split("=", 2);
          if (keyValue.length == 2) {
            properties.setProperty(keyValue[0].trim(), keyValue[1].trim());
          }
        }
      }
      return new ConnectionInfo(host, port, dbName, properties);
    }
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {

  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {

  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
