package io.ebean.datasource.test;

import io.ebean.datasource.DataSourceBuilder;
import io.ebean.datasource.DataSourcePool;
import io.ebean.test.containers.Db2Container;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test class shows a competitition between ONE connection pool that uses a DB2
 * trusted context and two connection pools with and without thread affinity
 */
@Disabled("DB2 container start is slow - run manually")
class Db2TrustedContextContainerTest {

  private static Db2Container container;

  private static Method dockerSuMethod = getSuMethod();

  private static ThreadLocal<Db2Tenant> currentTenant = new ThreadLocal<>();
  private static List<String> summary = new ArrayList<>();
  private static ExecutorService executor;

  private static final Db2Tenant[] TENANTS = {
    new Db2Tenant(1, "tenant1", null, "S1"),
    new Db2Tenant(2, "tenant2", null, "S2")
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


    Db2Container.Builder builder1 = Db2Container.builder("EGAL")
      .port(55506)
      .containerName("trusted_context_bad")
      .image("icr.io/db2_community/db2:12.0.0.0");
    Db2Container.Builder builder2 = Db2Container.builder("12.5.8.0")
      .port(55505)
      .containerName("trusted_context_good");

    container = builder1
      .dbName("unit")
      .user("unit")
      .password("unit")
      // to change collation, charset and other parameters like pagesize:
      .configOptions("USING CODESET UTF-8 TERRITORY DE COLLATE USING IDENTITY PAGESIZE 32768")
      .configOptions("USING STRING_UNITS CODEUNITS32")
      .build();

    container.start();

    setupTrustedContext("172.16.0.1"); // TODO: This will change per machine!
    executor = Executors.newCachedThreadPool();
  }

  @AfterAll
  static void after() {
    //container.stop();
    summary.forEach(System.out::println);
    executor.shutdown();
  }

  private static void setupTrustedContext(String localDockerIp) {
    // Step 0: reset trustedContext and drop tables from previous r
    dockerSu("admin", "db2 connect to unit;" +
      "db2 drop trusted context webapptrust;" +
      "db2 drop table S1.test;" +
      "db2 drop table S2.test;");

    // Step 1 create the users (may fail, if user already exists, but does not matter here)
    dockerSu("root", "useradd webuser");
    dockerSu("root", "echo \"webuser:webpass\" | chpasswd");
    dockerSu("root", "useradd tenant1");
    dockerSu("root", "useradd tenant2");

    // Step 2 create trusted context
    dockerSu("admin", "db2 connect to unit;" +
      "db2 create trusted context webapptrust based upon connection using system authid webuser attributes \\(address \\'" + localDockerIp + "\\'\\)  " +
      "WITH USE FOR " +
      "tenant1 WITHOUT AUTHENTICATION, " +
      "tenant2 WITHOUT AUTHENTICATION " +
      "ENABLE;" +
      "db2 grant connect on database to user webuser;" +
      "db2 create table WEBUSER.test \\(id int\\);" +
      "db2 create table S1.test \\(id int\\);" +
      "db2 create table S2.test \\(id int\\);" +
      "db2 insert into S1.test values \\(1\\);" +
      "db2 insert into S2.test values \\(2\\);" +
      "db2 grant all on schema WEBUSER to user webuser;" +
      "db2 grant all on schema S1 to user tenant1;" +
      "db2 grant all on schema S2 to user tenant2;" +
      "db2 grant connect, createtab on database to user tenant1;" +
      "db2 grant connect, createtab on database to user tenant2"
    );

  }

  private static void setupTrustedContext2(String localDockerIp) {
    // Step 0: reset trustedContext and drop tables from previous run
    dockerSu("admin", "db2 connect to unit;db2 drop trusted context webapptrust");
    dockerSu("admin", "db2 connect to unit;db2 drop table S1.test;db2 drop table S2.test;db2 drop table S3.test;db2 drop table S4.test;db2 drop table S5.test");

    // Step 1 create the users (may fail, if user already exists, but does not matter here)
    dockerSu("root", "useradd webuser");
    dockerSu("root", "echo \"webuser:webpass\" | chpasswd");
    for (int i = 1; i <= 2; i++) {
      dockerSu("root", "useradd tenant" + i);
      dockerSu("root", "echo \"tenant" + i + ":pass" + i + "\" | chpasswd");
    }


    // Step 2 create trusted context
    dockerSu("admin", "db2 connect to unit;" +
      "db2 create trusted context webapptrust based upon connection using system authid webuser attributes \\(address \\'" + localDockerIp + "\\'\\)  " +
      "WITH USE FOR " +
      "tenant1 WITHOUT AUTHENTICATION, " +
      "tenant2 WITHOUT AUTHENTICATION," + // tenant 2 needs password (for testing)
      "tenant3 WITHOUT AUTHENTICATION," +
      "tenant4 WITHOUT AUTHENTICATION," +
      "tenant5 WITHOUT AUTHENTICATION " +
      "ENABLE");

    // Step 3: webuser (=trusted context "root") has connect permissions
    dockerSu("admin", "db2 connect to unit;db2 grant connect on database to user webuser");

    for (int i = 1; i <= 2; i++) {
      dockerSu("admin", "db2 connect to unit;db2 create table S" + i + ".test \\(id int\\)");
      dockerSu("admin", "db2 connect to unit;db2 insert into S" + i + ".test values \\(" + i + "\\)");
      dockerSu("admin", "db2 connect to unit;db2 grant all on schema S" + i + " to user tenant" + i);
    }

    dockerSu("admin", "db2 connect to unit;db2 grant connect on database to user tenant1");
    dockerSu("admin", "db2 connect to unit;db2 grant connect on database to user tenant2");

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
    DataSourcePool pool = getPool(10);
    try {

      // set tenant of this thread to tenant1
      assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
      assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
      currentTenant.set(TENANTS[0]);

      // TestDDL
      pool.status(true);
      assertThat(executeQuery(pool, "select * from test")).isEqualTo(1); // each tenant must read its own data!
      assertThat(executeQuery(pool, "select * from test")).isEqualTo(1); // check cache hit
      assertThat(pool.status(false).hitCount()).isEqualTo(2);

      assertThatThrownBy(() -> executeQuery(pool, "select * from S2.test"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQLCODE=-551, SQLSTATE=42501, SQLERRMC=TENANT1;SELECT;S2.TEST");

      currentTenant.set(TENANTS[1]);
      assertThat(executeQuery(pool, "select * from S2.test")).isEqualTo(2);


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


      if (false) {
        try (Connection conn = pool.getConnection()) {
          try (Statement stmt = conn.createStatement()) {
            exec(stmt, "SELECT current user FROM SYSIBM.SYSDUMMY1");
            exec(stmt, "SELECT current sqlid FROM SYSIBM.SYSDUMMY1");
            exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES ORDER BY AUTHID,AUTHIDTYPE, PRIVILEGE");
            exec(stmt, "SELECT * FROM SYSCAT.ROLES ORDER BY ROLENAME,ROLEID");
            exec(stmt, "SELECT * FROM SYSCAT.SCHEMAAUTH");
            exec(stmt, "SELECT * FROM SYSCAT.TABLES ORDER BY TABSCHEMA, TABNAME, OWNER");
            exec(stmt, "SELECT * FROM SYSCAT.SCHEMATA ORDER BY SCHEMANAME, OWNER");
          }
        }
      } else {
        testDdl(pool, 999);
        assertThat(executeQuery(pool, "select * from test2")).isEqualTo(999);


        currentTenant.set(TENANTS[0]);
        testDdl(pool, 1);
        assertThat(executeQuery(pool, "select * from test2")).isEqualTo(1);

        currentTenant.set(TENANTS[1]);
        testDdl(pool, 2);
        assertThat(executeQuery(pool, "select * from test2")).isEqualTo(2);
      }
      //assertThat(executeQuery(pool, "select * from test")).isEqualTo(2);

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


  private static void testDdl(DataSourcePool pool, int value) throws SQLException {
    try (Connection conn = pool.getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        exec(stmt, "SELECT current user FROM SYSIBM.SYSDUMMY1");
        exec(stmt, "SELECT current sqlid FROM SYSIBM.SYSDUMMY1");
        exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current user");
        exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current sqlid");
        exec(stmt, "SELECT * FROM SYSCAT.ROLES");
        exec(stmt, "SELECT * FROM SYSCAT.SCHEMAAUTH");
      }
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("drop table test");
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


  private static DataSourcePool getPool(int size) throws SQLException {
    return DataSourceBuilder.create()

      .username("webuser")
      .password("webpass")
      .maxConnections(size)
      .listener(new Db2TtrustedConnectionListener(new LongAdder()))
      .dataSource(new Db2TrustedDatasoruce(container.jdbcUrl(), "webuser", "webpass"))
      .heartbeatFreqSecs(1)
      .affinityProvider(currentTenant::get)
      .waitTimeoutMillis(10000)
      .build();
  }


}
