package io.ebean.datasource.pool;

import java.util.ArrayList;
import java.util.List;

/**
 * A buffer designed especially to hold free pooled connections.
 * <p>
 * All thread safety controlled externally (by PooledConnectionQueue).
 * </p>
 */
final class FreeConnectionBuffer {


  private final Node free = new Node(null);

  FreeConnectionBuffer() {
    Node end = new Node(null);
    free.next = end;
    end.prev = free;
  }

  int size = 0;

  /**
   * Return the number of entries in the buffer.
   */
  int size() {
    return size;
  }

  /**
   * Return true if the buffer is empty.
   */
  boolean isEmpty() {
    return size == 0;
  }

  /**
   * Add connection to the free list.
   */
  void add(PooledConnection pc) {
    free.add(new Node(pc));
    size++;
  }

  /**
   * Remove a connection from the free list.
   */
  PooledConnection remove() {
    Node node = free.next;
    node.remove();
    size--;
    return node.pc;
  }

  /**
   * Close all connections in this buffer.
   */
  void closeAll(boolean logErrors) {
    List<PooledConnection> tempList = new ArrayList<>();
    while (size > 0) {
      tempList.add(remove());
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
        size--;
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
