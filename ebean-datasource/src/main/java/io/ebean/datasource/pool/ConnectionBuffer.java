package io.ebean.datasource.pool;

import java.util.ArrayList;
import java.util.List;

/**
 * A buffer designed especially to hold pooled connections (free and busy ones)
 * <p>
 * The buffer contains two linkedLists (free and busy connection nodes)
 * <p>
 * When a node from the free list is removed, the node is attached to the
 * PooledConnection, so that the node object can be reused. This avoids object
 * creation/gc during remove operations.
 * <p>
 * All thread safety controlled externally (by PooledConnectionQueue).
 * </p>
 */
final class ConnectionBuffer {

  private final Node free = new Node(null);
  private final Node busy = new Node(null);

  ConnectionBuffer() {
    Node end = new Node(null);
    free.next = end;
    end.prev = free;

    end = new Node(null);
    busy.next = end;
    end.prev = busy;
  }

  int freeSize = 0;
  int busySize = 0;

  /**
   * Return the number of entries in the buffer.
   */
  int freeSize() {
    return freeSize;
  }

  /**
   * Return the number of busy connections.
   */
  int busySize() {
    return busySize;
  }

  /**
   * Return true if the buffer is empty.
   */
  boolean hasFreeConnections() {
    return freeSize > 0;
  }

  /**
   * Adds a new connection to the free list.
   */
  void addFree(PooledConnection pc) {
    assert pc.busyNode() == null : "Connection seems not to be new";
    free.add(new Node(pc));
    freeSize++;
  }

  /**
   * Removes the connection from the busy list. (For full close)
   * Returns true, if this connection was part of the busy list or false, if not (or removed twice)
   */
  boolean removeBusy(PooledConnection c) {
    if (c.busyNode() == null) {
      return false;
    }
    c.busyNode().remove();
    busySize--;
    c.setBusyNode(null);
    return true;
  }

  /**
   * Moves the connection from the busy list to the free list.
   */
  boolean moveToFreeList(PooledConnection c) {
    Node node = c.busyNode();
    if (node == null) {
      return false;
    }
    node.remove();
    busySize--;
    free.add(node);
    freeSize++;
    c.setBusyNode(null);
    return true;
  }

  /**
   * Remove a connection from the free list. Returns <code>null</code> if there is not any.
   */
  PooledConnection popFree() {
    Node node = free.next;
    if (node.isEdgeNode()) {
      return null;
    }
    node.remove();
    freeSize--;
    node.pc.setBusyNode(node); // sets the node for reuse in "addBusy"
    return node.pc;
  }

  /**
   * Adds the connection to the busy list. The connection must be either new or popped from the free list.
   */
  int addBusy(PooledConnection c) {
    Node node = c.busyNode(); // we try to reuse the node to avoid object creation.
    if (node == null) {
      node = new Node(c);
      c.setBusyNode(node);
    }
    busy.add(node);
    busySize++;
    return busySize;
  }

  /**
   * Close all free connections in this buffer.
   */
  void closeAllFree(boolean logErrors) {
    List<PooledConnection> tempList = new ArrayList<>();
    PooledConnection c = popFree();
    while (c != null) {
      tempList.add(c);
      c = popFree();
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
    Node node = free.next;
    while (minSize-- > 0) {
      if (node.isEdgeNode()) {
        return 0;
      }
      node = node.next;
    }

    while (!node.isEdgeNode()) {
      Node current = node;
      node = node.next;

      if (current.pc.shouldTrim(usedSince, createdSince)) {
        current.remove();
        freeSize--;
        current.pc.closeConnectionFully(true);
        trimCount++;
      }
    }
    return trimCount;
  }

  void closeBusyConnections(long leakTimeMinutes) {
    long olderThanTime = System.currentTimeMillis() - (leakTimeMinutes * 60000);
    Log.debug("Closing busy connections using leakTimeMinutes {0}", leakTimeMinutes);
    Node node = busy.next;
    while (!node.isEdgeNode()) {
      Node current = node;
      node = node.next;

      PooledConnection pc = current.pc;
      //noinspection StatementWithEmptyBody
      if (pc.lastUsedTime() > olderThanTime) {
        // PooledConnection has been used recently or
        // expected to be longRunning so not closing...
      } else {
        current.remove();
        --busySize;
        closeBusyConnection(pc);
      }
    }
  }

  private void closeBusyConnection(PooledConnection pc) {
    try {
      Log.warn("DataSource closing busy connection? {0}", pc.fullDescription());
      System.out.println("CLOSING busy connection: " + pc.fullDescription());
      pc.closeConnectionFully(false);
    } catch (Exception ex) {
      Log.error("Error when closing potentially leaked connection " + pc.description(), ex);
    }
  }

  String busyConnectionInformation(boolean toLogger) {
    if (toLogger) {
      Log.info("Dumping [{0}] busy connections: (Use datasource.xxx.capturestacktrace=true  ... to get stackTraces)", busySize());
    }
    StringBuilder sb = new StringBuilder();
    Node node = busy.next;
    while (!node.isEdgeNode()) {
      PooledConnection pc = node.pc;
      node = node.next;
      if (toLogger) {
        Log.info("Busy Connection - {0}", pc.fullDescription());
      } else {
        sb.append(pc.fullDescription()).append("\r\n");
      }
    }
    return sb.toString();
  }


  static final class Node {

    private Node next;
    private Node prev;
    final PooledConnection pc;

    private Node(PooledConnection pc) {
      this.pc = pc;
    }

    private boolean isEdgeNode() {
      return pc == null;
    }

    private void add(Node node) {
      assert node.next == null : "Node already member of list";
      assert node.prev == null : "Node already member of list";
      node.next = next;
      node.prev = this;
      next.prev = node;
      next = node;
    }

    private void remove() {
      assert pc != null : "called remove on an edge node";
      next.prev = prev;
      prev.next = next;
      prev = null;
      next = null;
    }
  }
}
