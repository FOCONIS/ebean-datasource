package io.ebean.datasource.test;

import io.ebean.datasource.DataSourceBuilder;
import io.ebean.datasource.DataSourcePool;
import io.ebean.test.containers.CommandException;
import io.ebean.test.containers.Db2Container;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test the basic usage of DB2 trusted context.
 */
@Disabled("DB2 container start is slow - run manually")
class Db2TrustedContextContainerTest {

	private static Db2Container container;

	private static ThreadLocal<Db2Tenant> currentTenant = new ThreadLocal<>();
	private static String NO_PASSWORD = null;
	private static final Db2Tenant TENANT1 = new Db2Tenant(1, "tenant1", NO_PASSWORD, "S1"); // schema must be uppercase!
	private static final Db2Tenant TENANT2 = new Db2Tenant(2, "tenant2", NO_PASSWORD, "S2");

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
	static void before() throws SQLException {


		Db2Container.Builder builder = Db2Container.builder("12.1.1.0")
				.port(55506)
				.containerName("trusted_context_12");

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
		// Step 0: reset trustedContext and drop tables from previous run
		try (Connection conn = container.config().createAdminConnection()) {
			tryExec(conn, "drop trusted context webapptrust");
			tryExec(conn, "drop table webuser.test");
			tryExec(conn, "drop table webuser.test2");
			tryExec(conn, "drop table s1.test");
			tryExec(conn, "drop table s1.test2");
			tryExec(conn, "drop table s2.test");
			tryExec(conn, "drop table s2.test2");
		}


		// Step 1 create the users (may fail, if user already exists, but does not matter here)
		try {
			container.dockerSu("root", "useradd webuser");
			container.dockerSu("root", "echo \"webuser:webpass\" | chpasswd");
			container.dockerSu("root", "useradd tenant1"); // no passwd
			container.dockerSu("root", "useradd tenant2"); // no passwd
		} catch (CommandException e) {
			// user already exists
		}

		try (Connection conn = container.config().createAdminConnection()) {
			Statement stmt = conn.createStatement();
			// create the trusted context for IP
			stmt.execute("create trusted context webapptrust based upon connection using system authid webuser attributes (address '" + localDockerIp + "')  " +
					"with use for tenant1 without authentication, tenant2 without authentication enable");

			// grant access to schemas
			stmt.execute("grant all on schema webuser to user webuser");
			stmt.execute("grant all on schema s1 to user tenant1");
			stmt.execute("grant all on schema s2 to user tenant2");

			// For DB2-12 we must explicitly grant access to tablespace and allow createtab
			stmt.execute("grant use of tablespace userspace1 to user webuser, user tenant1, user tenant2");
			stmt.execute("grant connect, createtab on database to user webuser, user tenant1, user tenant2");

			// create some testdata
			stmt.execute("create table webuser.test (id int)");
			stmt.execute("create table s1.test (id int)");
			stmt.execute("create table s2.test (id int)");
			stmt.execute("insert into s1.test values (1)");
			stmt.execute("insert into s2.test values (2)");
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
			// here we are connected as WEBUSER
			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("WEBUSER ");

			// switch to tenant 1
			currentTenant.set(TENANT1);
			pool.status(true); // reset hitcount
			assertThat(executeQuery(pool, "select * from test")).isEqualTo(1); // each tenant must read its own data!
			assertThat(executeQuery(pool, "select * from test")).isEqualTo(1); // check cache hit
			assertThat(pool.status(false).hitCount()).isEqualTo(2);

			// test if tenant1 can access data from tenant2
			assertThatThrownBy(() -> executeQuery(pool, "select * from s2.test"))
					.isInstanceOf(SQLException.class)
					.hasMessageContaining("SQLCODE=-551, SQLSTATE=42501, SQLERRMC=TENANT1;SELECT;S2.TEST");

			currentTenant.set(TENANT2); // test with tenant2
			assertThat(executeQuery(pool, "select * from s2.test")).isEqualTo(2);

			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("TENANT2 ");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S2      ");
			currentTenant.set(TENANT1);
			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("TENANT1 ");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S1      ");
			currentTenant.set(TENANT2);
			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("TENANT2 ");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("S2      ");
			currentTenant.set(null);
			assertThat(executeQuery(pool, "select current user from sysibm.sysdummy1")).isEqualTo("WEBUSER ");
			assertThat(executeQuery(pool, "select current sqlid from sysibm.sysdummy1")).isEqualTo("WEBUSER ");


			// now check, if we can create a table
			currentTenant.set(null);
			testDdl(pool, 999);
			assertThat(executeQuery(pool, "select * from test2")).isEqualTo(999);

			currentTenant.set(TENANT1);
			testDdl(pool, 1);
			assertThat(executeQuery(pool, "select * from test2")).isEqualTo(1);

			currentTenant.set(TENANT2);
			testDdl(pool, 2);
			assertThat(executeQuery(pool, "select * from test2")).isEqualTo(2);

		} finally {
			pool.shutdown();
		}
	}


	private static void testDdl(DataSourcePool pool, int value) throws SQLException {
		try (Connection conn = pool.getConnection()) {
			try (Statement stmt = conn.createStatement()) {
				stmt.execute("drop table test"); // drop old table
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
