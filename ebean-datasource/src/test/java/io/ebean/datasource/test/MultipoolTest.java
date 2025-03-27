package io.ebean.datasource.test;

import io.ebean.datasource.*;
import io.ebean.test.containers.MariaDBContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled("run manually")
class MultipoolTest {

  private static MariaDBContainer container;

  private static ExecutorService executor;


  @BeforeAll
  static void before() throws SQLException {
    container = MariaDBContainer.builder("latest")
      .dbName("unit")
      .user("unit")
      .password("unit")
      .build();

    container.start();

    executor = Executors.newCachedThreadPool();

    try (Connection c = container.config().createAdminConnection()) {
      c.setAutoCommit(true);
      Statement stmt = c.createStatement();

      for (int i = 1; i <= 2; i++) {
        stmt.execute("drop database if exists database" + i);
        stmt.execute("drop user if exists 'user" + i + "'@'%'");
        stmt.execute("create database database" + i);
        stmt.execute("create user 'user" + i + "'@'%' identified by 'password" + i + "'");
        stmt.execute("grant all privileges on database" + i + ".* to 'user" + i + "'@'%'");
        stmt.execute("create table database" + i + ".test (id int)");
        stmt.execute("insert into database" + i + ".test values (" + i + ")");
      }

    }
  }

  @AfterAll
  static void after() {
    executor.shutdownNow();
  }

  /*static class PoolManager implements DataSourcePoolListener {
    List<DataSourcePool> pools = new ArrayList<>();
    Semaphore semaphore = new Semaphore(120);
    Random random = new Random();

    @Override
    public void onBeforeCreateConnection(DataSourcePool pool) {
      try {
        while (!semaphore.tryAcquire(50, TimeUnit.MILLISECONDS)) {
          System.out.println("trim required");
          pools.get(random.nextInt(pools.size())).forceTrim(25);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onAfterCloseConnection(DataSourcePool pool) {
      semaphore.release();
    }

  }

  private static PoolManager poolManager = new PoolManager();
*/

  Object executeQuery(DataSourcePool pool, String query) throws Exception {
    try (Connection conn = pool.getConnection()) {
      try (PreparedStatement pstmt = conn.prepareStatement(query)) {
        ResultSet rs = pstmt.executeQuery();
        assertThat(rs.next()).isTrue();
        return rs.getObject(1);
      } finally {
        conn.rollback();
      }
    }
  }

  @Test
  void testTwoPools() throws Exception {

    DataSourcePool pool1 = getPool(1);
    DataSourcePool pool2 = getPool(2);

    try {
      assertThat(executeQuery(pool1, "select id from test")).isEqualTo(1);
      assertThat(executeQuery(pool2, "select id from test")).isEqualTo(2);
    } finally {
      pool1.shutdown();
      pool2.shutdown();
    }

  }

  @Test
  void testSharedPools() throws Exception {
    SharedPoolManager poolManager = new SharedPoolManager(container.jdbcUrl());
    DataSourcePool pool = DataSourceBuilder.create()
      .maxConnections(5)
      .url(container.jdbcUrl())
      .username("unit")
      .password("unit")
      .listener(poolManager)
      .affinityProvider(poolManager::getCurrentTenant)
      .build();

    try {
      poolManager.setCurrentTenant(1);
      assertThat(executeQuery(pool, "select id from test")).isEqualTo(1);
      poolManager.setCurrentTenant(2);
      assertThat(executeQuery(pool, "select id from test")).isEqualTo(2);
      consumeConnections(poolManager,pool,15);
    } finally {
      pool.shutdown();
    }
    System.out.println("Performed context switches: " + poolManager.getContextSwitches());

  }

  void consumeConnections(SharedPoolManager poolManager, DataSourcePool pool, int connectionsCount) throws Exception {
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < connectionsCount; i++) {
      int tenant = i % 2 + 1;
      Future<Boolean> submit = executor.submit(() -> {
        poolManager.setCurrentTenant(tenant);
        for (int j =0; j < 100000; j++) {
          try (Connection conn = pool.getConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement("select id from test")) {
              ResultSet rs = pstmt.executeQuery();
              assertThat(rs.next()).isTrue();
            }
            Thread.sleep(1);
            conn.rollback();
          }
        }
        return true;
      });
      futures.add(submit);
    }

    for (Future<?> future : futures) {
      future.get();
    }
  }

  private static DataSourcePool getPool(int id) {
    DataSourcePool pool = DataSourceBuilder.create()
      .url(container.jdbcUrl().replace("/unit", "/database" + id))
      .username("user" + id)
      .password("password" + id)
      .maxConnections(100)
      .build();
    return pool;
  }

  static class SharedPoolManager implements DataSourcePoolListener {
    private final ThreadLocal<Integer> currentTenant = new ThreadLocal<>();
    private final String baseUrl;
    private int contextSwitches;

    public SharedPoolManager(String baseUrl) {
      this.baseUrl = baseUrl;
    }

    public Integer getCurrentTenant() {
      return currentTenant.get();
    }

    public void setCurrentTenant(int currentTenant) {
      this.currentTenant.set(currentTenant);
    }

    public int getContextSwitches() {
      return contextSwitches;
    }

    @Override
    public Connection initConnection(DataSourcePool pool, Connection conn) {
      return new SharedConnection(conn);
    }

    @Override
    public void onAfterBorrowConnection(DataSourcePool pool, DataSourceConnection connection) throws SQLException {
      Integer tenant = (Integer) connection.affinityId();
      SharedConnection sc = connection.unwrap(SharedConnection.class);
      // do we have to perform a context switch of the shared connection.

      if (!Objects.equals(sc.currentTenant, tenant)) {
        // (This is quick and dirty with no error handling)
        connection.clearPreparedStatementCache();
        boolean ac = sc.currentConnection.getAutoCommit();
        int level = sc.getTransactionIsolation();
        sc.currentConnection.close();
        System.out.println("CLOS Connection: " + System.identityHashCode(sc.currentConnection));
        sc.currentConnection = DriverManager.getConnection(baseUrl.replace("/unit", "/database" + tenant), "user" + tenant, "password" + tenant);
        System.out.println("OPEN Connection: " + System.identityHashCode(sc.currentConnection));
        sc.setAutoCommit(ac);
        sc.setTransactionIsolation(level);
        sc.currentTenant = tenant;
        contextSwitches++;
      }
    }
  }

  /**
   * delegate class.
   */
  static class SharedConnection implements Connection {
    private Integer currentTenant;
    private Connection currentConnection;

     SharedConnection(Connection currentConnection) {
      this.currentConnection = currentConnection;
    }

    @Override
    public Statement createStatement() throws SQLException {
      return currentConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
      return currentConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
      return currentConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
      return currentConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
      currentConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
      return currentConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
      currentConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
      currentConnection.rollback();
    }

    @Override
    public void close() throws SQLException {
      currentConnection.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
      return currentConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      return currentConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
      currentConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
      return currentConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
      currentConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
      return currentConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
      currentConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
      return currentConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      return currentConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
      currentConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      return currentConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return currentConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return currentConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
      return currentConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      currentConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
      currentConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
      return currentConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
      return currentConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
      return currentConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
      currentConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      currentConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return currentConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return currentConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return currentConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return currentConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return currentConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      return currentConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
      return currentConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
      return currentConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
      return currentConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
      return currentConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
      return currentConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
      currentConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
      currentConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
      return currentConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
      return currentConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return currentConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return currentConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
      currentConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
      return currentConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
      currentConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      currentConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
      return currentConnection.getNetworkTimeout();
    }

    @Override
    public void beginRequest() throws SQLException {
      currentConnection.beginRequest();
    }

    @Override
    public void endRequest() throws SQLException {
      currentConnection.endRequest();
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
      return currentConnection.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
      return currentConnection.setShardingKeyIfValid(shardingKey, timeout);
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
      currentConnection.setShardingKey(shardingKey, superShardingKey);
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
      currentConnection.setShardingKey(shardingKey);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface == SharedConnection.class) {
        return (T) this;
      }
      return currentConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      if (iface == SharedConnection.class) {
        return true;
      }
      return currentConnection.isWrapperFor(iface);
    }
  }
}
