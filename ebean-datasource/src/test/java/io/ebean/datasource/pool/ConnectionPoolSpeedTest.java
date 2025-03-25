package io.ebean.datasource.pool;

import io.ebean.datasource.DataSourceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ConnectionPoolSpeedTest {

  private final Logger logger = LoggerFactory.getLogger(ConnectionPoolSpeedTest.class);

  private final ConnectionPool pool;

  private final Random random = new Random();

  private int total;

  ConnectionPoolSpeedTest() {
    pool = createPool();
  }

  private ConnectionPool createPool() {

    DataSourceConfig config = new DataSourceConfig();
    config.setDriver("org.h2.Driver");
    config.setUrl("jdbc:h2:mem:tests");
    config.setUsername("sa");
    config.setPassword("");
    config.setMinConnections(2);
    config.setMaxConnections(100);
    config.setAutoCommit(true);
    config.affinityProvider(Thread::currentThread);

    return new ConnectionPool("testspeed", config);
  }

  @AfterEach
  public void after() {
    pool.shutdown();
  }

  @Test
  public void test() throws SQLException {

    warm();

    total = 0;

    long startNano = System.nanoTime();

    perform();

    long exeNano = System.nanoTime() - startNano;
    long avgNanos = exeNano / total;

    logger.info("avgNanos[{}] total[{}]", avgNanos, total);

    assertThat(avgNanos).isLessThan(300);
  }

  @Test
  public void testMultiThread() throws Exception {
    warm();

    total = 0;
    List<Thread> threads = new ArrayList<>();
    for (int threadCount = 0; threadCount < 100; threadCount++) {
      threads.add(createThread());
    }

    long startNano = System.nanoTime();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    long exeNanos = System.nanoTime() - startNano;

    logger.info("exeNanos[{}]", exeNanos);
  }

  private Thread createThread() {
    return new Thread(() -> {
      try {
        for (int j = 0; j < 500; j++) {
          for (int k = 0; k < 20; k++) {
            try (Connection conn = pool.getConnection()) {
              try (PreparedStatement stmt = conn.prepareStatement("select '" + Thread.currentThread().getName() + "', " + k)) {
                stmt.execute();
              }
              conn.rollback();
            }
          }
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    });
  }

  private void perform() throws SQLException {
    for (int i = 0; i < 1_000_000; i++) {
      getAndCloseSome();
    }
  }

  private void warm() throws SQLException {
    for (int i = 0; i < 50; i++) {
      getAndCloseSome();
    }
  }

  private void getAndCloseSome() throws SQLException {

    int num = random.nextInt(5);
    Connection[] bunch = new Connection[num];
    for (int i = 0; i < num; i++) {
      bunch[i] = pool.getConnection();
    }

    for (Connection connection : bunch) {
      connection.close();
    }

    total += num;
  }

}
