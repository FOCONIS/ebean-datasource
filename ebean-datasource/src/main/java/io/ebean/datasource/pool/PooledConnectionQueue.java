package io.ebean.datasource.pool;

import io.ebean.datasource.ConnectionPoolExhaustedException;
import io.ebean.datasource.PoolStatus;
import io.ebean.datasource.pool.ConnectionPool.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.Logger.Level.DEBUG;

final class PooledConnectionQueue {

  private static final TimeUnit MILLIS_TIME_UNIT = TimeUnit.MILLISECONDS;
  private static final long MAX_METRICS_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(59);

  private final String name;
  private final ConnectionPool pool;
  /**
   * A buffer designed specifically for free and busy connections.
   */
  private final ConnectionBuffer buffer;
  /**
   * Main lock guarding all access
   */
  private final ReentrantLock lock;
  /**
   * Condition for threads waiting to take a connection
   */
  private final Condition notEmpty;
  private int connectionId;
  private final long waitTimeoutMillis;
  private final long maxAgeMillis;
  private final int minSize;
  private int maxSize;
  private int creatingConnections;
  private long resetGeneration;
  private long shutdownGeneration;
  /**
   * Number of threads in the wait queue.
   */
  private int waitingThreads;
  /**
   * Number of times a thread had to wait.
   */
  private int waitCount;
  /**
   * Number of times a connection was got from this queue.
   */
  private int hitCount;
  private int lastWaitCount;
  private int lastHitCount;
  private long totalAcquireNanos;
  private long maxAcquireNanos;
  private long totalWaitNanos;
  private long lastTotalAcquireNanos;
  private long lastTotalWaitNanos;
  private final LongSupplier nanoTime;
  private long lastMaxResetNanos;
  private int publishedHighWaterMark;
  private long publishedMaxAcquireNanos;

  /**
   * The high water mark for the queue size.
   */
  private int highWaterMark;
  /**
   * Last time the pool was reset. Used to close busy connections as they are
   * returned to the pool that where created prior to the lastResetTime.
   */
  private long lastResetTime;
  private boolean doingShutdown;
  private final long validateStaleMillis;

  PooledConnectionQueue(ConnectionPool pool, LongSupplier nanoTime) {
    this.pool = pool;
    this.name = pool.name();
    this.minSize = pool.minSize();
    this.maxSize = pool.maxSize();
    this.waitTimeoutMillis = pool.waitTimeoutMillis();
    this.maxAgeMillis = pool.maxAgeMillis();
    this.validateStaleMillis = pool.validateStaleMillis();
    this.buffer = new ConnectionBuffer(pool.affinitySize());
    this.lock = new ReentrantLock(false);
    this.notEmpty = lock.newCondition();
    this.nanoTime = nanoTime;
    this.lastMaxResetNanos = nanoTime.getAsLong() - MAX_METRICS_WINDOW_NANOS;
  }

  private PoolStatus createStatus() {
    return new Status(minSize, maxSize, buffer.freeSize(), buffer.busySize(), waitingThreads, highWaterMark,
      waitCount, hitCount, totalAcquireNanos, maxAcquireNanos, totalWaitNanos);
  }

  @Override
  public String toString() {
    lock.lock();
    try {
      return createStatus().toString();
    } finally {
      lock.unlock();
    }
  }

  PoolStatus status(boolean reset) {
    lock.lock();
    try {
      PoolStatus s = createStatus();
      if (reset) {
        resetMetrics(nanoTime.getAsLong());
      }
      return s;
    } finally {
      lock.unlock();
    }
  }

  PoolStatus collect(boolean delta) {
    lock.lock();
    try {
      var now = nanoTime.getAsLong();
      if (now - lastMaxResetNanos >= MAX_METRICS_WINDOW_NANOS) {
        publishedHighWaterMark = highWaterMark;
        publishedMaxAcquireNanos = maxAcquireNanos;
        resetMaxMetrics(now);
      }
      var collectedWaitCount = delta ? waitCount - lastWaitCount : waitCount;
      var collectedHitCount = delta ? hitCount - lastHitCount : hitCount;
      var collectedTotalAcquireNanos = delta ? totalAcquireNanos - lastTotalAcquireNanos : totalAcquireNanos;
      var collectedTotalWaitNanos = delta ? totalWaitNanos - lastTotalWaitNanos : totalWaitNanos;
      var status = new Status(minSize, maxSize, buffer.freeSize(), buffer.busySize(), waitingThreads, publishedHighWaterMark,
        collectedWaitCount, collectedHitCount, collectedTotalAcquireNanos, publishedMaxAcquireNanos, collectedTotalWaitNanos);
      if (delta) {
        lastWaitCount = waitCount;
        lastHitCount = hitCount;
        lastTotalAcquireNanos = totalAcquireNanos;
        lastTotalWaitNanos = totalWaitNanos;
      }
      return status;
    } finally {
      lock.unlock();
    }
  }

  private void resetMetrics(long now) {
    highWaterMark = buffer.busySize();
    hitCount = 0;
    waitCount = 0;
    maxAcquireNanos = 0;
    totalAcquireNanos = 0;
    totalWaitNanos = 0;
    lastHitCount = 0;
    lastWaitCount = 0;
    lastTotalAcquireNanos = 0;
    lastTotalWaitNanos = 0;
    publishedHighWaterMark = highWaterMark;
    publishedMaxAcquireNanos = maxAcquireNanos;
    lastMaxResetNanos = now;
  }

  private void resetMaxMetrics(long now) {
    highWaterMark = buffer.busySize();
    maxAcquireNanos = 0;
    lastMaxResetNanos = now;
  }

  void setMaxSize(int maxSize) {
    lock.lock();
    try {
      if (maxSize < this.minSize) {
        throw new IllegalArgumentException("maxSize " + maxSize + " < minSize " + this.minSize);
      }
      this.maxSize = maxSize;
    } finally {
      lock.unlock();
    }
  }

  private int totalConnections() {
    return buffer.freeSize() + buffer.busySize();
  }

  void createConnections(int numberToAdd) throws SQLException {
    lock.lock();
    try {
      for (int i = 0; i < numberToAdd; i++) {
        buffer.addFree(pool.createConnectionForQueue(connectionId++));
      }
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return a PooledConnection.
   */
  void returnPooledConnection(PooledConnection c, boolean forceClose) {
    boolean closeConnection = false;
    lock.lock();
    try {
      if (!buffer.removeBusy(c)) {
        Log.warn("Connection [{0}] not found in BusyList?", c);
      }
      if (forceClose || c.shouldTrimOnReturn(lastResetTime, maxAgeMillis)) {
        closeConnection = true;
      } else {
        buffer.addFree(c);
        notEmpty.signal();
      }
    } finally {
      lock.unlock();
    }
    if (closeConnection) {
      c.closeConnectionFully(false);
    }
  }

  private PooledConnection extractFromFreeList(Object affinitiyId) {

    PooledConnection c = buffer.removeFree(affinitiyId);
    if (c == null) {
      return null;
    }
    if (validateStaleMillis > 0 && staleEviction(c)) {
      c.closeConnectionFully(false);
      return null;
    }
    registerBusyConnection(c);
    return c;
  }

  private boolean staleEviction(PooledConnection c) {
    if (!stale(c)) {
      return false;
    }
    if (Log.isLoggable(DEBUG)) {
      Log.debug("stale connection validation lastUsed:{0}ms stale:{1}ms", (System.currentTimeMillis() - c.lastUsedTime()), validateStaleMillis);
    }
    return pool.invalidConnection(c);
  }

  private boolean stale(PooledConnection c) {
    return c.lastUsedTime() < System.currentTimeMillis() - validateStaleMillis;
  }

  PooledConnection obtainConnection(Object affinitiyId) throws SQLException {
    try {
      PooledConnection pc = _obtainConnection(affinitiyId);
      pc.resetForUse();
      return pc;

    } catch (InterruptedException e) {
      // restore the interrupted status as we throw SQLException
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted getting connection from pool", e);
    }
  }

  /**
   * Register the PooledConnection with the busyList.
   */
  private int registerBusyConnection(PooledConnection connection) {
    int busySize = buffer.addBusy(connection);
    if (busySize > highWaterMark) {
      highWaterMark = busySize;
    }
    return busySize;
  }

  private PooledConnection _obtainConnection(Object affinitiyId) throws InterruptedException, SQLException {
    var start = System.nanoTime();
    lock.lockInterruptibly();
    try {
      if (doingShutdown) {
        throw new SQLException("Trying to access the Connection Pool when it is shutting down");
      }
      // exclude heartbeat from application metrics
      if (affinitiyId != ConnectionBuffer.HEARTBEAT) {
        hitCount++;
      }
      // are other threads already waiting? (they get priority)
      if (waitingThreads == 0) {
        PooledConnection connection = extractFromFreeList(affinitiyId);
        if (connection != null) {
          return connection;
        }
        connection = createConnection();
        if (connection == null && buffer.isAffinitySupported()) {
          // we could not find connection with required affinity and
          // buffer is full. So try to get oldest connection from buffer
          connection = extractFromFreeList(ConnectionBuffer.GET_OLDEST);
        }
        if (connection != null) {
          return connection;
        }
      }
      try {
        // The pool is at maximum size. We are going to go into
        // a wait loop until connections are returned into the pool.
        if (affinitiyId != ConnectionBuffer.HEARTBEAT) {
          waitCount++;
        }
        waitingThreads++;
        return _obtainConnectionWaitLoop(null);
      } finally {
        waitingThreads--;
        if (affinitiyId != ConnectionBuffer.HEARTBEAT) {
          totalWaitNanos += (System.nanoTime() - start);
        }
      }
    } finally {
      if (affinitiyId != ConnectionBuffer.HEARTBEAT) {
        final var elapsed = System.nanoTime() - start;
        totalAcquireNanos += elapsed;
        maxAcquireNanos = Math.max(maxAcquireNanos, elapsed);
      }
      lock.unlock();
    }
  }

  private PooledConnection createConnection() throws SQLException {
    if (totalConnections() + creatingConnections >= maxSize) {
      return null;
    }
    int id = connectionId++;
    long generation = shutdownGeneration;
    creatingConnections++;
    lock.unlock();
    try {
      return createConnectionOutsideLock(id, generation);
    } finally {
      lock.lock();
    }
  }

  private PooledConnection createConnectionOutsideLock(int id, long generation) throws SQLException {
    PooledConnection connection = null;
    boolean close = false;
    try {
      connection = pool.createConnectionForQueue(id);
    } finally {
      lock.lock();
      try {
        creatingConnections--;
        if (connection == null || doingShutdown || generation != shutdownGeneration) {
          close = connection != null;
        } else {
          int busySize = registerBusyConnection(connection);
          if (Log.isLoggable(DEBUG)) {
            Log.debug("DataSource [{0}] grow; id[{1}] free[{2}] busy[{3}] max[{4}]", name, connection.name(), buffer.freeSize(), busySize, maxSize);
          }
        }
      } finally {
        lock.unlock();
      }
    }
    if (close) {
      connection.closeConnectionFully(false);
      throw new SQLException("Connection pool was reset or shut down while creating a connection");
    }
    return connection;
  }

  /**
   * Got into a loop waiting for connections to be returned to the pool.
   */
  private PooledConnection _obtainConnectionWaitLoop(Object affinitiyId) throws SQLException, InterruptedException {
    long nanos = MILLIS_TIME_UNIT.toNanos(waitTimeoutMillis);
    for (; ; ) {
      if (nanos <= 0) {
        // We waited long enough, that a connection was returned, so we try to create a new connection.
        PooledConnection conn = createConnection();
        if (conn != null) {
          return conn;
        }
        String msg = "Unsuccessfully waited [" + waitTimeoutMillis + "] millis for a connection to be returned."
          + " No connections are free. You need to Increase the max connections of [" + maxSize + "]"
          + " or look for a connection pool leak using datasource.xxx.captureStackTrace=true";
        if (pool.captureStackTrace()) {
          dumpBusyConnectionInformation();
        }

        throw new ConnectionPoolExhaustedException(msg);
      }

      try {
        nanos = notEmpty.awaitNanos(nanos);
        PooledConnection c = extractFromFreeList(affinitiyId);
        if (c == null && buffer.isAffinitySupported()) {
          c = extractFromFreeList(ConnectionBuffer.GET_OLDEST);
        }
        if (c != null) {
          // successfully waited
          return c;
        }
      } catch (InterruptedException ie) {
        notEmpty.signal(); // propagate to non-interrupted thread
        throw ie;
      }
    }
  }

  PoolStatus shutdown(boolean closeBusyConnections) {
    lock.lock();
    try {
      doingShutdown = true;
      resetGeneration++;
      shutdownGeneration++;
      PoolStatus status = createStatus();
      closeFreeConnections(true);

      if (!closeBusyConnections) {
        // connections close on return to pool
        lastResetTime = System.currentTimeMillis() - 100;
      } else {
        if (buffer.busySize() > 0) {
          Log.warn("Closing busy connections on shutdown size: {0}", buffer.busySize());
          dumpBusyConnectionInformation();
          closeBusyConnections(0);
        }
      }
      return status;
    } finally {
      lock.unlock();
      doingShutdown = false;
    }
  }

  /**
   * Close all the connections in the pool and any current busy connections
   * when they are returned. New connections will be then created on demand.
   * <p>
   * This is typically done when a database down event occurs.
   */
  void reset(long leakTimeMinutes) {
    lock.lock();
    try {
      resetGeneration++;
      PoolStatus status = createStatus();
      Log.info("Resetting DataSource [{0}] {1}", name, status);
      lastResetTime = System.currentTimeMillis();

      closeFreeConnections(false);
      closeBusyConnections(leakTimeMinutes);

      String busyInfo = getBusyConnectionInformation();
      if (!busyInfo.isEmpty()) {
        Log.info("Busy Connections:\n {0}", busyInfo);
      }

    } finally {
      lock.unlock();
    }
  }

  void trim(long maxInactiveMillis, long maxAgeMillis) {
    int firstConnectionId = -1;
    int add;
    long generation = 0;
    List<PooledConnection> trimmedConnections;
    lock.lock();
    try {
      trimmedConnections = trimInactiveConnections(maxInactiveMillis, maxAgeMillis);
      int freeDeficit = minSize - buffer.freeSize();
      int capacity = maxSize - totalConnections() - creatingConnections;
      add = Math.min(freeDeficit, capacity);
      if (add > 0) {
        firstConnectionId = connectionId;
        connectionId += add;
        creatingConnections += add;
        generation = resetGeneration;
      }
    } finally {
      lock.unlock();
    }
    for (var connection : trimmedConnections) {
      connection.closeConnectionFully(true);
    }
    if (add > 0) {
      createReservedConnections(firstConnectionId, add, generation);
    }
  }

  private void createReservedConnections(int firstConnectionId, int numberToAdd, long generation) {
    for (int i = 0; i < numberToAdd; i++) {
      PooledConnection connection = null;
      boolean close = false;
      try {
        try {
          connection = pool.createConnectionForQueue(firstConnectionId + i);
        } catch (SQLException e) {
          Log.error("Error trying to create a free connection", e);
        }
      } finally {
        lock.lock();
        try {
          creatingConnections--;
          if (connection == null || doingShutdown || generation != resetGeneration
            || buffer.freeSize() >= minSize || totalConnections() >= maxSize) {
            close = connection != null;
          } else {
            buffer.addFree(connection);
            if (Log.isLoggable(DEBUG)) {
              Log.debug("DataSource [{0}] grow reserve; id[{1}] free[{2}] busy[{3}] max[{4}]", name, connection.name(), buffer.freeSize(), buffer.busySize(), maxSize);
            }
            notEmpty.signal();
          }
        } finally {
          lock.unlock();
        }
      }
      if (close) {
        connection.closeConnectionFully(false);
      }
    }
  }

  /**
   * Trim connections that have been not used for some time.
   */
  private List<PooledConnection> trimInactiveConnections(long maxInactiveMillis, long maxAgeMillis) {
    final long createdSince = (maxAgeMillis == 0) ? 0 : System.currentTimeMillis() - maxAgeMillis;
    final List<PooledConnection> trimmedConnections;
    if (buffer.freeSize() > minSize) {
      // trim on maxInactive and maxAge
      long usedSince = System.currentTimeMillis() - maxInactiveMillis;
      int excess = buffer.freeSize() - minSize;
      // Progressively reduce excess idle connections rather than closing them all at once.
      int maxTrim = Math.max(1, (excess + 4) / 5);
      trimmedConnections = buffer.trim(minSize, usedSince, createdSince, maxTrim);
    } else if (createdSince > 0) {
      // trim only on maxAge
      trimmedConnections = buffer.trim(0, createdSince, createdSince, Integer.MAX_VALUE);
    } else {
      trimmedConnections = List.of();
    }
    if (!trimmedConnections.isEmpty() && Log.isLoggable(DEBUG)) {
      Log.debug("DataSource [{0}] trim [{1}] inactive connections. free[{2}] busy[{3}]",
        name, trimmedConnections.size(), buffer.freeSize(), buffer.busySize());
    }
    return trimmedConnections;
  }

  /**
   * Close all the connections that are in the free list.
   */
  private void closeFreeConnections(boolean logErrors) {
    List<PooledConnection> tempList;
    lock.lock();
    try {
      tempList = buffer.clearFreeList();
    } finally {
      lock.unlock();
    }
    // closing the connections is done outside lock
    if (Log.isLoggable(System.Logger.Level.TRACE)) {
      Log.trace("... closing all {0} connections from the free list with logErrors: {1}", tempList.size(), logErrors);
    }
    for (PooledConnection connection : tempList) {
      connection.closeConnectionFully(logErrors);
    }
  }

  /**
   * Close any busy connections that have not been used for some time.
   * <p>
   * These connections are considered to have leaked from the connection pool.
   * <p>
   * Connection leaks occur when code doesn't ensure that connections are
   * closed() after they have been finished with. There should be an
   * appropriate try catch finally block to ensure connections are always
   * closed and put back into the pool.
   */
  void closeBusyConnections(long leakTimeMinutes) {
    List<PooledConnection> tempList;
    lock.lock();
    try {
      tempList = buffer.cleanupBusyList(leakTimeMinutes);
    } finally {
      lock.unlock();
    }
    for (PooledConnection pc : tempList) {
      try {
        Log.warn("DataSource closing busy connection? {0}", pc.fullDescription());
        pc.closeConnectionFully(false);
      } catch (Exception ex) {
        Log.error("Error when closing potentially leaked connection " + pc.description(), ex);
      }
    }
  }

  String getBusyConnectionInformation() {
    return getBusyConnectionInformation(false);
  }

  void dumpBusyConnectionInformation() {
    getBusyConnectionInformation(true);
  }

  /**
   * Returns information describing connections that are currently being used.
   */
  private String getBusyConnectionInformation(boolean toLogger) {
    lock.lock();
    try {
      return buffer.busyConnectionInformation(toLogger);
    } finally {
      lock.unlock();
    }
  }

}
