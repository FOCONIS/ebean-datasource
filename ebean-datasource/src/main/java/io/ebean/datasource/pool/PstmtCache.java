package io.ebean.datasource.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A LRU based cache for PreparedStatements.
 */
final class PstmtCache extends LinkedHashMap<String, ExtendedPreparedStatement> {

  private static final Logger logger = LoggerFactory.getLogger(PstmtCache.class);

  static final long serialVersionUID = -3096406924865550697L;

  private final int maxSize;
  private int removeCounter;
  private int hitCounter;
  private int missCounter;
  private int putCounter;

  PstmtCache(int maxCacheSize) {
    // note = access ordered list.  This is what gives it the LRU order
    super(maxCacheSize * 3, 0.75f, true);
    this.maxSize = maxCacheSize;
  }

  /**
   * Return a summary description of this cache.
   */
  String getDescription() {
    return "size[" + size() + "] max[" + maxSize + "] hits[" + hitCounter + "] miss[" + missCounter + "] hitRatio[" + getHitRatio() + "] removes[" + removeCounter + "]";
  }

  /**
   * returns the current maximum size of the cache.
   */
  public int getMaxSize() {
    return maxSize;
  }

  /**
   * Gets the hit ratio.  A number between 0 and 100 indicating the number of
   * hits to misses.  A number approaching 100 is desirable.
   */
  private int getHitRatio() {
    if (hitCounter == 0) {
      return 0;
    } else {
      return hitCounter * 100 / (hitCounter + missCounter);
    }
  }

  /**
   * The total number of hits against this cache.
   */
  public int getHitCounter() {
    return hitCounter;
  }

  /**
   * The total number of misses against this cache.
   */
  public int getMissCounter() {
    return missCounter;
  }

  /**
   * The total number of puts against this cache.
   */
  public int getPutCounter() {
    return putCounter;
  }

  /**
   * Try to add the returning statement to the cache. If there is already a
   * matching ExtendedPreparedStatement in the cache return false else add
   * the statement to the cache and return true.
   */
  boolean returnStatement(ExtendedPreparedStatement stmt) {
    ExtendedPreparedStatement alreadyInCache = super.get(stmt.getCacheKey());
    if (alreadyInCache != null) {
      return false;
    }
    // add the returning prepared statement to the cache.
    // Note that the LRUCache will automatically close fully old unused
    // statements when the cache has hit its maximum size.
    put(stmt.getCacheKey(), stmt);
    return true;
  }

  /**
   * additionally maintains hit and miss statistics.
   */
  @Override
  public ExtendedPreparedStatement get(Object key) {
    ExtendedPreparedStatement o = super.get(key);
    if (o == null) {
      missCounter++;
    } else {
      hitCounter++;
    }
    return o;
  }

  /**
   * additionally maintains hit and miss statistics.
   */
  @Override
  public ExtendedPreparedStatement remove(Object key) {
    ExtendedPreparedStatement o = super.remove(key);
    if (o == null) {
      missCounter++;
    } else {
      hitCounter++;
    }
    return o;
  }

  /**
   * additionally maintains put counter statistics.
   */
  @Override
  public ExtendedPreparedStatement put(String key, ExtendedPreparedStatement value) {
    putCounter++;
    return super.put(key, value);
  }

  /**
   * will check to see if we need to remove entries and
   * if so call the cacheCleanup.cleanupEldestLRUCacheEntry() if
   * one has been set.
   */
  @Override
  protected boolean removeEldestEntry(Map.Entry<String, ExtendedPreparedStatement> eldest) {
    if (size() < maxSize) {
      return false;
    }
    removeCounter++;
    try {
      ExtendedPreparedStatement stmt = eldest.getValue();
      stmt.closeDestroy();
    } catch (SQLException e) {
      logger.error("Error closing ExtendedPreparedStatement", e);
    }
    return true;
  }

}

