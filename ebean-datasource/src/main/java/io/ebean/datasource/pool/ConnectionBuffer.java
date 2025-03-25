package io.ebean.datasource.pool;

import java.util.ArrayList;
import java.util.List;

/**
 * A buffer designed especially to hold pooled connections (free and busy ones)
 * <p>
 * All thread safety controlled externally (by PooledConnectionQueue).
 * </p>
 */
final class ConnectionBuffer {


  private final Node free = new Node(null);

  ConnectionBuffer() {
    Node end = new Node(null);
    free.next = end;
    end.prev = free;
  }

  int freeSize = 0;

  /**
   * Return the number of entries in the buffer.
   */
  int freeSize() {
    return freeSize;
  }

  /**
   * Return true if the buffer is empty.
   */
  boolean hasFreeConnections() {
    return freeSize > 0;
  }

  /**
   * Add connection to the free list.
   */
  void addFree(PooledConnection pc) {
    free.add(new Node(pc));
    freeSize++;
  }

  /**
   * Remove a connection from the free list. Returns <code>null</code> if there is not any.
   */
  Node popFree () {
    Node node = free.next;
    if (node.isEgeNode()) {
      return null;
    }
    node.remove();
    freeSize--;
    return node;
  }

  /**
   * Close all connections in this buffer.
   */
  void closeAll(boolean logErrors) {
    List<PooledConnection> tempList = new ArrayList<>();
    Node node = popFree();
    while (node != null) {
      tempList.add(node.pc);
      node = popFree();
    }

    if (Log.isLoggable(System.Logger.Level.TRACE)) {
      Log.trace("... closing all {0} connections from the free list with logErrors: {1}", tempList.size(), logErrors);
    }
    for (PooledConnection connection : tempList) {
      connection.closeConnectionFully(logErrors);
    }
  }

  /**
   * Trim any inactive connections that have not been used since usedSince.
   */
  int trim(int minSize, long usedSince, long createdSince) {
    int trimCount = 0;
    Node trimFrom = free.next;
    while (minSize-- > 0) {
      if (trimFrom.isEgeNode()) {
        return 0;
      }
      trimFrom = trimFrom.next;
    }

    while (!trimFrom.isEgeNode()) {

      if (trimFrom.pc.shouldTrim(usedSince, createdSince)) {
        trimFrom.remove();
        freeSize--;
        trimFrom.pc.closeConnectionFully(true);
        trimCount++;
      }
      trimFrom = trimFrom.next;
    }
    return trimCount;
  }

  static class Node {

    Node next;
    Node prev;
    final PooledConnection pc;

    Node(PooledConnection pc) {
      this.pc = pc;
    }

    boolean isEgeNode() {
      return pc == null;
    }

    public void add(Node node) {
      node.next = next;
      node.prev = this;
      next.prev = node;
      next = node;
    }

    public void remove() {
      assert pc != null : "called remove on an edge node";
      next.prev = prev;
      prev.next = next;
    }
  }
}
