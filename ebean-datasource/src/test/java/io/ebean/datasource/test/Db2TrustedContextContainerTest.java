package io.ebean.datasource.test;

import io.ebean.datasource.DataSourceBuilder;
import io.ebean.datasource.DataSourcePool;
import io.ebean.test.containers.CommandException;
import io.ebean.test.containers.Db2Container;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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

	private static ThreadLocal<Db2Tenant> currentTenant = new ThreadLocal<>();

	private static final Db2Tenant[] TENANTS = {
			new Db2Tenant(1, "mandant1", null, "S1"),
			new Db2Tenant(2, "mandant2", null, "S2")
	};

	/**
	 * Unfortunately, container.dockerSu is protected. So we use some reflection in the meantime
	 */
	static void dockerSu(String user, String cmd) {
		System.out.println("dockerSu: " + user + ", " + cmd);
		List<String> ret = container.dockerSu(user, cmd);
		System.out.println("OK: " + ret);

	}

	/**
	 * Setup the DB2 docker with trusted context support
	 */
	@BeforeAll
	static void before() throws InvocationTargetException, IllegalAccessException, SQLException {



/*		Db2Container.Builder builder = Db2Container.builder("11.5.9.0")
				.port(55505)
				.containerName("trusted_context_good");*/

		Db2Container.Builder builder = Db2Container.builder("12.1.1.0")
				.port(55506)
				.containerName("trusted_context_bad");

		container = builder
				.dbName("unit")
				.user("unit")
				.password("unit")
				// to change collation, charset and other parameters like pagesize:
				.configOptions("USING CODESET UTF-8 TERRITORY DE COLLATE USING IDENTITY PAGESIZE 32768")
				.configOptions("USING STRING_UNITS CODEUNITS32")
				.build();

		container.start();

		setupTrustedContext("172.16.0.1"); // TODO: This will change per machine!
	}

	@AfterAll
	static void after() {
		//container.stop();
	}

	private static void tryExec(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
			conn.commit();
			System.out.println("Exec: " + sql + "; OK");
		} catch (SQLException e) {
			System.out.println("Exec: " + sql + "; FAIL: " + e.getMessage());
		}

	}

	private static void setupTrustedContext(String localDockerIp) throws SQLException {
		// Step 0: reset trustedContext and drop tables from previous r
		try (Connection conn = container.config().createAdminConnection()) {
			tryExec(conn, "drop trusted context webapptrust");
			tryExec(conn, "drop table WEBUSER.test");
			tryExec(conn, "drop table WEBUSER.test2");
			tryExec(conn, "drop table S1.test");
			tryExec(conn, "drop table S1.test2");
			tryExec(conn, "drop table S2.test");
			tryExec(conn, "drop table S2.test2");
		}


		// Step 1 create the users (may fail, if user already exists, but does not matter here)
		try {
			container.dockerSu("root", "useradd webuser");
			container.dockerSu("root", "echo \"webuser:webpass\" | chpasswd");
			container.dockerSu("root", "useradd mandant1");
			container.dockerSu("root", "useradd mandant2");
		} catch (CommandException e) {
			// user already exists
		}
		try (Connection conn = container.config().createAdminConnection()) {
			Statement stmt = conn.createStatement();
			stmt.execute("create trusted context webapptrust based upon connection using system authid webuser attributes (address '" + localDockerIp + "')  " +
					"WITH USE FOR " +
					"mandant1 WITHOUT AUTHENTICATION, " +
					"mandant2 WITHOUT AUTHENTICATION " +
					"ENABLE");
			//stmt.execute("grant connect, createtab on database to user webuser");
			stmt.execute("create table WEBUSER.test (id int)");
			stmt.execute("create table S1.test (id int)");
			stmt.execute("create table S2.test (id int)");
			stmt.execute("insert into S1.test values (1)");
			stmt.execute("insert into S2.test values (2)");
			stmt.execute("grant all on schema WEBUSER to user webuser");
			stmt.execute("grant all on schema S1 to user mandant1");
			stmt.execute("grant all on schema S2 to user mandant2");
			stmt.execute("grant connect on database to user mandant1");
			stmt.execute("grant connect on database to user mandant2");

			stmt.execute("GRANT USE OF TABLESPACE USERSPACE1 TO user WEBUSER, user MANDANT1, user MANDANT2");
			stmt.execute("GRANT CONNECT, CREATETAB on database TO user WEBUSER, user MANDANT1, user MANDANT2");

			//stmt.execute("GRANT USE OF TABLESPACE USERSPACE1 TO user MANDANT1 WITH GRANT OPTION");
			//stmt.execute("GRANT USE OF TABLESPACE USERSPACE1 TO user MANDANT2 WITH GRANT OPTION");
		/*	tryExec(conn, "CREATE TABLESPACE DATATS");
			tryExec(conn, "CREATE TEMPORARY TABLESPACE TEMPDATA");
			stmt.execute(" GRANT USE OF TABLESPACE DATATS TO user MANDANT1 WITH GRANT OPTION");
			stmt.execute(" GRANT USE OF TABLESPACE DATATS TO user MANDANT2 WITH GRANT OPTION");
*/
		}
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
//			try (Connection conn = pool.getConnection("webuser", "webpass")) {
//				conn.createStatement().execute("select * from test");
//				conn.createStatement().execute("create table test4 (id int) in DATATS");
//				conn.commit();
//			}
			try (Connection conn = pool.getConnection()) {
				conn.createStatement().execute("create table test2 (id int)");
			}

			// set tenant of this thread to mandant1
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
					.hasMessageContaining("SQLCODE=-551, SQLSTATE=42501, SQLERRMC=MANDANT1;SELECT;S2.TEST");

			currentTenant.set(TENANTS[1]);
			assertThat(executeQuery(pool, "select * from S2.test")).isEqualTo(2);


			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("MANDANT2");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S2      ");
			currentTenant.set(TENANTS[0]);
			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("MANDANT1");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S1      ");
			currentTenant.set(TENANTS[1]);
			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("MANDANT2");
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
				try (Connection conn = pool.getConnection()) {
					try (Statement stmt = conn.createStatement()) {
						stmt.execute("create table S1.test2 (id int) in USERSPACE1");
					}
				}
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
			/*try (Statement stmt = conn.createStatement()) {
				exec(stmt, "SELECT current user FROM SYSIBM.SYSDUMMY1");
				exec(stmt, "SELECT current sqlid FROM SYSIBM.SYSDUMMY1");
				exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current user");
				exec(stmt, "SELECT * FROM SYSIBMADM.PRIVILEGES WHERE AUTHID = current sqlid");
				exec(stmt, "SELECT * FROM SYSCAT.ROLES");
				exec(stmt, "SELECT * FROM SYSCAT.SCHEMAAUTH");
				exec(stmt, "SELECT * FROM SYSCAT.TABLES");
			}*/
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("drop table test");
				try {
					stmt.execute("drop table test2");
					conn.commit();
				} catch (SQLException e) {
					// Table did not exist
				}
				stmt.execute("create table test2 (id int) in USERSPACE1");
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
