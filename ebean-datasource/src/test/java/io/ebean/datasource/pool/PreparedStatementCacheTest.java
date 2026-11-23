package io.ebean.datasource.pool;

import io.ebean.datasource.DataSourceConfig;
import org.h2.Driver;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class PreparedStatementCacheTest {

  /**
   * Nach dem Austausch der physischen Verbindung muss dieselbe SQL-Abfrage ein neues
   * PreparedStatement erhalten und den Wert aus der zweiten Datenbank liefern.
   */
  @Test
  void queryAfterPhysicalConnectionSwitch() throws SQLException {
    var firstUrl = "jdbc:h2:mem:preparedStatementCacheFirst";
    try (var first = DriverManager.getConnection(firstUrl, "sa", "");
         var second = DriverManager.getConnection("jdbc:h2:mem:preparedStatementCacheSecond")) {
      populate(first, 10);
      populate(second, 20);

      var delegate = new AtomicReference<>(first);
      var forwarding = forwardingConnection(delegate);

      var config = new DataSourceConfig()
        .setUrl(firstUrl)
        .setUsername("sa")
        .setPassword("")
        .setMinConnections(1)
        .setMaxConnections(1)
        .setAutoCommit(true)
        .setPstmtCacheSize(2)
        .validateOnHeartbeat(false);
      config.driver(new Driver() {
        @Override
        public Connection connect(String url, Properties info) {
          return forwarding;
        }
      });
      var pool = new ConnectionPool("preparedStatementCache", config);
      try {
        Connection firstBorrow;
        try (var connection = pool.getConnection()) {
          firstBorrow = connection;
          assertThat(query(connection)).isEqualTo(10);
        }

        try (var connection = pool.getConnection()) {
          assertThat(connection).isSameAs(firstBorrow);
          ((PooledConnection) connection).clearPreparedStatementCache();
          first.close();
          delegate.set(second);

          assertThat(query(connection)).isEqualTo(20);
        }
      } finally {
        pool.shutdown();
      }
    }
  }

  private static void populate(Connection connection, int value) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute("create table test (id int primary key, test_value int)");
      statement.execute("insert into test values (1, " + value + ")");
    }
  }

  private static int query(Connection connection) throws SQLException {
    try (var statement = connection.prepareStatement("select test_value from test where id = ?")) {
      statement.setInt(1, 1);
      try (var result = statement.executeQuery()) {
        assertThat(result.next()).isTrue();
        return result.getInt(1);
      }
    }
  }

  private static Connection forwardingConnection(AtomicReference<Connection> delegate) {
    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
      (proxy, method, args) -> {
        try {
          return method.invoke(delegate.get(), args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      });
  }
}
