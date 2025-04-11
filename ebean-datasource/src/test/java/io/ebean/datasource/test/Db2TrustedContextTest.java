package io.ebean.datasource.test;

import io.ebean.datasource.DataSourceBuilder;
import io.ebean.datasource.DataSourcePool;
import io.ebean.test.containers.Db2Container;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test class shows a competitition between ONE connection pool that uses a DB2
 * trusted context and two connection pools with and without thread affinity
 */
@Disabled("DB2 container start is slow - run manually")
class Db2TrustedContextTest {

  private static Db2Container container;

  private static Method dockerSuMethod = getSuMethod();

  private static ThreadLocal<Db2Tenant> currentTenant = new ThreadLocal<>();
  private static List<String> summary = new ArrayList<>();
  private static ExecutorService executor;

  private static final Db2Tenant[] TENANTS = {
    new Db2Tenant(1, "tenant1", null, "S1"),
    new Db2Tenant(2, "tenant2", "pass2", "S2"),
    new Db2Tenant(3, "tenant3", null, "S3"),
    new Db2Tenant(4, "tenant4", null, "S4"),
    new Db2Tenant(5, "tenant5", null, "S5"),
  };

   /**
   * Unfortunately, container.dockerSu is protected. So we use some reflection in the meantime
   */
  private static Method getSuMethod() {
    try {
      Method m = Db2Container.class.getDeclaredMethod("dockerSu", String.class, String.class);
      m.setAccessible(true);
      return m;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Unfortunately, container.dockerSu is protected. So we use some reflection in the meantime
   */
  static void dockerSu(String user, String cmd) {
    System.out.println("dockerSu: " + user + ", " + cmd);
    try {
      List<String> ret = (List<String>) dockerSuMethod.invoke(container, user, cmd);
      System.out.println("OK: " + ret);
    } catch (InvocationTargetException ite) {
      System.err.println("FAIL: " + ite.getCause().getMessage());
    } catch (Exception e) {
      System.err.println("FAIL: ");
      e.printStackTrace();
    }
  }

  /**
   * Setup the DB2 docker with trusted context support
   */
  @BeforeAll
  static void before() throws InvocationTargetException, IllegalAccessException {
    container = Db2Container.builder("11.5.8.0")
      .port(55506)
      .containerName("trusted_context_gut")
     .image("icr.io/db2_community/db2:11.5.8.0")
      .dbName("unit")
      .user("unit")
      .password("unit")
      // to change collation, charset and other parameters like pagesize:
      .configOptions("USING CODESET UTF-8 TERRITORY DE COLLATE USING IDENTITY PAGESIZE 32768")
      .configOptions("USING STRING_UNITS CODEUNITS32")
      .build();

    container.start();

   // setupTrustedContext("172.16.0.1"); // TODO: This will change per machine!
    executor = Executors.newCachedThreadPool();
  }

  @AfterAll
  static void after() {
    //container.stop();
    summary.forEach(System.out::println);
    executor.shutdown();
  }

  private static void setupTrustedContext(String localDockerIp) {
    // Step 0: reset trustedContext and drop tables from previous run
    dockerSu("admin", "db2 connect to unit;db2 drop trusted context webapptrust");
    dockerSu("admin", "db2 connect to unit;db2 drop table S1.test;db2 drop table S2.test;db2 drop table S3.test;db2 drop table S4.test;db2 drop table S5.test");

    // Step 1 create the users (may fail, if user already exists, but does not matter here)
    dockerSu("root", "useradd webuser");
    dockerSu("root", "echo \"webuser:webpass\" | chpasswd");
    for (int i = 1; i <= 5; i++) {
      dockerSu("root", "useradd tenant" + i);
      dockerSu("root", "echo \"tenant" + i + ":pass" + i + "\" | chpasswd");
    }


    // Step 2 create trusted context
    dockerSu("admin", "db2 connect to unit;" +
      "db2 create trusted context webapptrust based upon connection using system authid webuser attributes \\(address \\'" + localDockerIp + "\\'\\)  " +
      "WITH USE FOR " +
      "tenant1 WITHOUT AUTHENTICATION, " +
      "tenant2 WITH AUTHENTICATION," + // tenant 2 needs password (for testing)
      "tenant3 WITHOUT AUTHENTICATION," +
      "tenant4 WITHOUT AUTHENTICATION," +
      "tenant5 WITHOUT AUTHENTICATION " +
      "ENABLE");

    // Step 3: webuser (=trusted context "root") has connect permissions
    dockerSu("admin", "db2 connect to unit;db2 grant connect on database to user webuser");

    for (int i = 1; i <= 5; i++) {
      dockerSu("admin", "db2 connect to unit;db2 create table S" + i + ".test \\(id int\\)");
      dockerSu("admin", "db2 connect to unit;db2 insert into S" + i + ".test values \\(" + i + "\\)");
      dockerSu("admin", "db2 connect to unit;db2 grant all on schema S" + i + " to user tenant" + i);
    }

    dockerSu("admin", "db2 connect to unit;db2 grant connect on database to user tenant1");
    dockerSu("admin", "db2 connect to unit;db2 grant connect on database to user tenant2");

  }

  private LongAdder successCount = new LongAdder();
  private LongAdder queryCount = new LongAdder();
  private LongAdder latency = new LongAdder();
  private LongAdder switchCount = new LongAdder();
  private volatile boolean running = true;


  enum LoadProfile {
    /**
     * try to do as much as work you can
     */
    MAX_LOAD,
    /**
     * Perform 100 queries and hold connection for 10ms
     */
    HOLD,
    /**
     * Perform "future-find" bursts
     */
    BURST;
  }

  void doSomeWork(DataSourcePool pool, Db2Tenant tenant, LoadProfile loadProfile) {
    currentTenant.set(tenant);
    try {
      switch (loadProfile) {
        case MAX_LOAD:
          while (running) {
            assertThat(executeQuery(pool, "select * from test")).isEqualTo(tenant.id());
            queryCount.increment();
          }
          break;
        case HOLD:
          for (int i = 0; i < 10; i++) {
            long start = System.nanoTime();
            try (Connection conn = pool.getConnection()) {
              latency.add(System.nanoTime() - start);
              Thread.sleep(10);
              conn.rollback();
            }
            queryCount.increment();
          }
          break;
        case BURST:
          for (int i = 0; i < 10; i++) {
            List<Future<Object>> futures = new ArrayList<>();
            long start = System.nanoTime();
            for (int j = 0; j < 10; j++) {
              futures.add(executor.submit(() -> {
                currentTenant.set(tenant);
                return executeQuery(pool, "select * from test");
              }));
            }
            for (Future<Object> future : futures) {
              assertThat(future.get()).isEqualTo(tenant.id());
            }
            latency.add(System.nanoTime() - start);
            Thread.sleep(10);
          }

      }
      successCount.increment();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  Thread createWorkerThreas(DataSourcePool pool, Db2Tenant tenant, LoadProfile loadProfile) {
    Thread thread = new Thread(() -> {
      doSomeWork(pool, tenant, loadProfile);
    });
    return thread;
  }

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
  void testTrustedContext() throws Exception {
    DataSourcePool pool = getPool(10, false, switchCount);
    try {
      try (Connection conn = pool.getConnection()) {
        try(Statement stmt = conn.createStatement()) {
          exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current user");
          exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current sqlid");
          exec(stmt, "SELECT * FROM SYSCAT.ROLES");
        }
      }
      // set tenant of this thread to tenant1
      assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
      assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
      currentTenant.set(TENANTS[0]);

      try (Connection conn = pool.getConnection()) {
        try(Statement stmt = conn.createStatement()) {
          exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current user");
          exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current sqlid");
          exec(stmt, "SELECT * FROM SYSCAT.ROLES");
        }
      }
      // TestDDL
      pool.status(true);
      assertThat(executeQuery(pool, "select * from test")).isEqualTo(1); // each tenant must read its own data!
      assertThat(executeQuery(pool, "select * from test")).isEqualTo(1); // check cache hit
      assertThat(pool.status(false).hitCount()).isEqualTo(2);

      testDdl(pool, 1);
      assertThat(executeQuery(pool, "select * from test2")).isEqualTo(1);

      assertThatThrownBy(() -> executeQuery(pool, "select * from S2.test"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQLCODE=-551, SQLSTATE=42501, SQLERRMC=TENANT1;SELECT;S2.TEST");

      currentTenant.set(TENANTS[1]);
      assertThat(executeQuery(pool, "select * from S2.test")).isEqualTo(2);

      testDdl(pool, 2);
      assertThat(executeQuery(pool, "select * from test")).isEqualTo(2);
      assertThat(executeQuery(pool, "select * from test2")).isEqualTo(2);

      assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("TENANT2 ");
      assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S2      ");
      currentTenant.set(TENANTS[0]);
      assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("TENANT1 ");
      assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S1      ");
      currentTenant.set(TENANTS[1]);
      assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("TENANT2 ");
      assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S2      ");
      currentTenant.set(null);
      assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
      assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
    } finally {
      pool.shutdown();
    }
  }

  private static void exec(Statement stmt, String sql) throws SQLException {
    System.out.println("=============================================================");
    System.out.println(sql);
    ResultSet resultSet = stmt.executeQuery(sql);
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columns = metaData.getColumnCount();
    for (int i = 1; i <= columns; i++) {
      System.out.print(metaData.getColumnName(i) + "\t");
    }
    System.out.println();

    // Print table body
    while (resultSet.next()) {
      for (int i = 1; i <= columns; i++) {
        System.out.print(resultSet.getString(i) + "\t");
      }
      System.out.println();
    }
  }


  @Test
  void testAffinity() throws Exception {
    DataSourcePool pool = getPool(10, true, switchCount);
    try {
      // set tenant of this thread to tenant1
      currentTenant.set(TENANTS[0]);
      Connection connection1 = pool.getConnection();
      connection1.rollback();
      connection1.close();

      Connection connection2 = pool.getConnection();
      connection2.rollback();
      connection2.close();

      assertThat(connection1).isSameAs(connection2);

      currentTenant.set(TENANTS[1]);
      Connection connection3 = pool.getConnection();
      connection3.rollback();
      connection3.close();

      assertThat(connection3).isNotSameAs(connection1);


      currentTenant.set(TENANTS[0]);
      Connection connection4 = pool.getConnection();
      connection4.rollback();
      connection4.close();

      assertThat(connection4).isSameAs(connection1);

    } finally {
      pool.shutdown();
    }
  }
  @Test
  void testUnterschied() throws Exception {
    DataSourcePool pool = getPoolForTenant(5, 1, true);
    DataSourcePool tcPool = getPool(10, false, switchCount);

    Connection connection1 = pool.getConnection();
    Connection connection2 = tcPool.getConnection();

    System.out.println(connection1);
    System.out.println(connection2);

  }
  @Test
  void testTwoPools() throws Exception {
    DataSourcePool pool1 = getPoolForTenant(5, 1, true);
    DataSourcePool pool2 = getPoolForTenant(5, 2, true);
    try {
      // set tenant of this thread to tenant1
      pool1.status(true);
      assertThat(executeQuery(pool1, "select * from test")).isEqualTo(1); // each tenant must read its own data!
      assertThat(executeQuery(pool1, "select * from test")).isEqualTo(1); // check cache hit
      assertThat(pool1.status(false).hitCount()).isEqualTo(2);

      assertThatThrownBy(() -> executeQuery(pool1, "select * from S2.test"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQLCODE=-551, SQLSTATE=42501, SQLERRMC=TENANT1;SELECT;S2.TEST");

      testDdl(pool1, 1);
      assertThat(executeQuery(pool1, "select * from test2")).isEqualTo(1);

      assertThat(executeQuery(pool2, "select * from S2.test")).isEqualTo(2);

      testDdl(pool2, 2);
      assertThat(executeQuery(pool2, "select * from test")).isEqualTo(2);
      assertThat(executeQuery(pool2, "select * from test2")).isEqualTo(2);
    } finally {
      pool1.shutdown();
      pool2.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("testKeys")
  void testThroughputTrustedContext(TestKey testKey) throws Exception {
    switchCount.reset();
    DataSourcePool pool = getPool(testKey.poolSize, testKey.affinity, switchCount);
    String prefix = testKey.toString().replaceAll(",", "");
    try {
      latency.reset();
      long qps = checkThroughput(List.of(pool), testKey.threads, testKey.loadProfile);
      summary.add(prefix + "\t" + qps + "\t" + latency.longValue() + "\t" + switchCount.longValue() + "\tswitch");
    } catch (Throwable t) {
      summary.add(prefix + "\tFAIL\t0\t0\tswitch");
      throw t;
    } finally {
      pool.shutdown();
    }
  }


  @ParameterizedTest
  @MethodSource("testKeys")
  void testThroughputMultiplePools(TestKey testKey) throws Exception {
    String prefix = testKey.toString().replaceAll(",", "");
    List<DataSourcePool> pools = new ArrayList<>();
    for (int id = 1; id <= TENANTS.length; id++) {
      pools.add(getPoolForTenant(testKey.poolSize / TENANTS.length, id, testKey.affinity));
    }
    try {
      latency.reset();
      long qps = checkThroughput(pools, testKey.threads, testKey.loadProfile);
      summary.add(prefix + "\t" + qps + "\t" + latency.longValue() + "\t0\ttwoPools");
    } catch (Throwable t) {
      summary.add(prefix + "\tFAIL\t0\t0\ttwoPools");
      throw t;
    } finally {
      pools.forEach(DataSourcePool::shutdown);
    }
  }

  private static void testDdl(DataSourcePool pool, int value) throws SQLException {
    try (Connection conn = pool.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        try {
          stmt.execute("drop table test2");
          conn.commit();
        } catch (SQLException e) {
          // Table did not exist
        }
        stmt.execute("create table test2 (id int)");
        try (PreparedStatement pstmt = conn.prepareStatement("insert into test2 values (?)")) {
          pstmt.setInt(1, value);
          pstmt.executeUpdate();
        }
      } finally {
        conn.commit();
      }
    }
  }


  private long checkThroughput(List<DataSourcePool> pools, int threadCount, LoadProfile loadProfile) throws InterruptedException {
    successCount.reset();

    long time = System.currentTimeMillis();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      threads.add(createWorkerThreas(pools.get(i % pools.size()), TENANTS[i % TENANTS.length], loadProfile));
    }
    running = true;
    for (Thread thread : threads) {
      thread.start();
    }
    if (loadProfile == LoadProfile.MAX_LOAD) {
      Thread.sleep(1000);
    }
    running = false;
    for (Thread thread : threads) {
      thread.join();
    }
    time = System.currentTimeMillis() - time;
    long qps = queryCount.longValue() * 1000L / time;
    System.out.println("Success: " + successCount.longValue() + ", QPS: " + qps);
    assertThat(successCount.longValue()).isEqualTo(threadCount);
    return qps;
  }

  static List<TestKey> testKeys() {
    List<TestKey> keys = new ArrayList<>();
    int[] threadsList = {1, 5, 10, 20, 50, 100};
    int[] poolSizeList = {10, 20, 50};
    for (int threads : threadsList) {
      for (int pools : poolSizeList) {
        for (LoadProfile profile : LoadProfile.values()) {
          keys.add(new TestKey(pools, threads, profile, false));
          keys.add(new TestKey(pools, threads, profile, true));
        }
      }
    }
    return keys;
  }

  static class TestKey {
    final int poolSize;
    final int threads;
    final LoadProfile loadProfile;
    final boolean affinity;

    TestKey(int poolSize, int threads, LoadProfile loadProfile, boolean affinity) {
      this.poolSize = poolSize;
      this.threads = threads;
      this.loadProfile = loadProfile;
      this.affinity = affinity;
    }

    @Override
    public String toString() {
      return poolSize + ",\t" + threads + ",\t" + loadProfile + ",\t" + affinity;
    }
  }

  private static DataSourcePool getPool(int size, boolean affinity, LongAdder switchCount) throws SQLException {
    return DataSourceBuilder.create()

      .username("webuser")
      .password("webpass")
      .maxConnections(size)
      .listener(new Db2TtrustedConnectionListener(switchCount))
      .dataSource(new Db2TrustedDatasoruce(container.jdbcUrl(), "webuser", "webpass"))
      .heartbeatFreqSecs(1)
      .affinityProvider(currentTenant::get)
      .affinitySize(affinity ? 257 : 0)
      .waitTimeoutMillis(10000)
      .build();
  }

  private static DataSourcePool getPoolForTenant(int size, int id, boolean affinity) {
    return DataSourceBuilder.create()
      .url(container.jdbcUrl() + ":currentSchema=S" + id + ";")
      //.username("tenant" + id)
      //.password("pass" + id)
      .username("webuser")
      .password("webpass")
      .maxConnections(size)
      .affinityProvider(Thread::currentThread)
      .affinitySize(affinity ? 257 : 0)
      .build();
  }

}
