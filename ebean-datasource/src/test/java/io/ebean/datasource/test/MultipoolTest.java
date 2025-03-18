package io.ebean.datasource.test;

import io.ebean.datasource.DataSourceBuilder;
import io.ebean.datasource.DataSourcePool;
import io.ebean.datasource.DataSourcePoolListener;
import io.ebean.test.containers.MariaDBContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Disabled("run manually")
class MultipoolTest {

	private static MariaDBContainer container;

	private static ExecutorService executor;

	@BeforeAll
	static void before() {
		container = MariaDBContainer.builder("latest")
				.dbName("unit")
				.user("unit")
				.password("unit")
				.build();

		container.start();

		executor = Executors.newCachedThreadPool();

	}

	@AfterAll
	static void after() {
		executor.shutdownNow();
	}

	static class PoolManager implements DataSourcePoolListener {

	}

	private static PoolManager poolManager = new PoolManager();

	@Test
	void testFalseFriendRollback() throws Exception {

		DataSourcePool pool1 = getPool();
		DataSourcePool pool2 = getPool();

		try {
			consumeConnections(pool1, 100);
			consumeConnections(pool2, 100);
		} finally {
			pool1.shutdown();
			pool2.shutdown();
		}

	}

	void consumeConnections(DataSourcePool pool, int connectionsCount) throws Exception {
		List<Future<?>> futures = new ArrayList<>();

		for (int i = 0; i < connectionsCount; i++) {
			Future<Boolean> submit = executor.submit(() -> {
				try (Connection conn = pool.getConnection()) {
					Thread.sleep(1000);
					conn.rollback();
				}
				return true;
			});
			futures.add(submit);
		}

		for (Future<?> future : futures) {
			future.get();
		}
	}

	private static DataSourcePool getPool() {
		return DataSourceBuilder.create()
				.url(container.jdbcUrl())
				.username("unit")
				.password("unit")
				.ownerUsername("unit")
				.ownerPassword("unit")
				.maxConnections(100)
				.listener(poolManager)
				.build();
	}
}
