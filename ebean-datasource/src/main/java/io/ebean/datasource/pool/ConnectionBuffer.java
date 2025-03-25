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
 * The connectionbuffer iself has one linkedList from <code>free</code> to
 * <code>freeEnd</code>. In parallel, the elements in this list can also be part
 * the affinityNodes list, which implement a kind of hashmap.
 * <p>
 * So you can prefer which connection should be taken. You can use CurrentThread or
 * currentTenant as affinity ID. So you likely get a connection that has the right
 * pstatement caches or is already in the CPU cache.
 * <p>
 * Without affinityId, the first free-connection is taken.
 * <p>
 * With affinityId, the affinityNodes-list is determined by the hashCode, then the
 * list is searched, if there is a connection with the same affinity object.
 * <p>
 * If there is no one found, we take the LAST connection in freeList, as this is
 * the best candidate not to steal the affinity of a connection, that was currently
 * used. This ensures (or also causes) that the pool has at least that size of the
 * frequent used affinityIds. E.g. if the affinity id represents tenant id, and
 * 15 tenants are active, the pool will not shrink below 15 - on the other hand,
 * there is always one connection ready for each active tenant.
 * <p>
 * A free node can be member in two lists:
 * <ol>
 *     <li>it is definitively member in the freeList</li>
 *     <li>it may be member in one of the affinity-lists (mod hash)</li>
 * </ol>
 * The remove / transition from free to busy will remove the node from both lists.
 * <p>
 * Graphical exammple
 * <pre>
 *     By default, the busy list is empty
 *     busy ---------------------------------------------------> busyEnd
 *     free --> c1 --> c2 --> c3 --> c4 --> c5 --> c6 --> c7 --> freeEnd
 *     al1  ---------------------------------------------------> end
 *     al2  ---------------------------------------------------> end
 *     ...
 *     al257---------------------------------------------------> end
 *
 *     if a popFree(1) is called, we lookup in al1 and found no usable node.
 *     in this case, we take the last node, c7 and move it to the busy list
 *
 *     busy --> c7 --------------------------------------------> busyEnd
 *     free --> c1 --> c2 --> c3 --> c4 --> c5 --> c6 ---------> freeEnd
 *
 *     When we put that node back in the freelist, it becomes the first node
 *     and it will be also linked in affinity-list1
 *
 *     busy ---------------------------------------------------> busyEnd
 *     free ,       ,> c1 --> c2 --> c3 --> c4 --> c5 --> c6 --> freeEnd
 *     al1-> '> c7 '
 *     al2-> (empty)
 *
 *     subsequent popFree(1) will always return c7 as long as it is not busy.
 *     now we call popFree(1) twice, we will get this picture
 *
 *     busy --> c6 --> c7 ----------------------------------------> busyEnd
 *     free --> c1 --> c2 --> c3 --> c4 --> c5 -------------------> freeEnd
 *     al1-> (empty)
 *     al2-> (empty)
 *
 *     putting them back
 *
 *     busy ------------------------------------------------------> busyEnd
 *     free ,             ,> c1 --> c2 --> c3 --> c4 --> c5 -----> freeEnd
 *     al1-> '> c7 -- c6 '
 *     al2-> (empty)
 *
 *     fetching and return a connection with affinity = 2:
 *
 *     busy ------------------------------------------------------> busyEnd
 *     free ,                   ,> c1 --> c2 --> c3 --> c4 -------> freeEnd
 *     al1-> |     '> c7 -- c6 '
 *     al2-> '> c2 '
 *
 *     so we have 2 connections for affinity 1 and one connection for affinity 2
 *     (and the rest is ordered itself in the freeList)
 * </pre>
 * <p>
 * All thread safety controlled externally (by PooledConnectionQueue).
 * </p>
 */
final class ConnectionBuffer {

    private final Node free = Node.init();
    private final Node freeEnd = free.next;
    private final Node busy = Node.init();

    private final Node[] affinityNodes;
    private final int hashSize;

    ConnectionBuffer(int hashSize) {
        this.hashSize = hashSize;
        if (hashSize > 0) {
            affinityNodes = new Node[hashSize];
            for (int i = 0; i < affinityNodes.length; i++) {
                affinityNodes[i] = Node.init();
            }
        } else {
            affinityNodes = null;
        }
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
        new Node(pc).addTo(free);
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

        Object affinityId = c.affinityId();
        if (affinityId != null) {
            node.addTo(free, affinityNodes[affinityId.hashCode() % hashSize]);
        } else {
            node.addTo(free);
        }
        freeSize++;
        c.setBusyNode(null);
        return true;
    }

    /**
     * Remove a connection from the free list. Returns <code>null</code> if there is not any.
     * <p>
     * Connections that are returend from this method must be either added to busyList with
     * addBusy or closed fully.
     */
    PooledConnection popFree(Object affinityId) {
        Node node;
        if (affinityId == null || affinityNodes == null) {
            node = free.next;
        } else {
            node = affinityNodes[affinityId.hashCode() % hashSize].find(affinityId);
            if (node == null) {
                // when we did not find a node with that affinity, we take the last (oldest one)
                // and reuse this with the new affinity. This avoids to "steal" the affinity
                // from the newest one.
                node = freeEnd.prev;
            }
        }
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
        node.addTo(busy);
        busySize++;
        return busySize;
    }

    /**
     * Close all free connections in this buffer.
     */
    void closeAllFree(boolean logErrors) {
        List<PooledConnection> tempList = new ArrayList<>();
        PooledConnection c = popFree(null);
        while (c != null) {
            tempList.add(c);
            c = popFree(null);
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
        private Node nextAffinity;
        private Node prevAffinity;
        final PooledConnection pc;

        private Node(PooledConnection pc) {
            this.pc = pc;
        }

        /**
         * Creates new "list" with two empty edge nodes
         */
        public static Node init() {
            Node node1 = new Node(null);
            Node node2 = new Node(null);
            node1.next = node2;
            node2.prev = node1;
            node1.nextAffinity = node2;
            node2.prevAffinity = node1;
            return node1;
        }

        private boolean isEdgeNode() {
            return pc == null;
        }

        private void remove() {
            assert pc != null : "called remove on an edge node";
            assert prev != null && next != null : "not part of a list";
            next.prev = prev;
            prev.next = next;
            prev = null;
            next = null;
            if (nextAffinity != null) {
                nextAffinity.prevAffinity = prevAffinity;
                prevAffinity.nextAffinity = nextAffinity;
                prevAffinity = null;
                nextAffinity = null;
            }
        }

        public void addTo(Node list, Node affinityList) {
            addTo(list);
            assert nextAffinity == null : "Node already member of list";
            assert prevAffinity == null : "Node already member of list";
            nextAffinity = affinityList.nextAffinity;
            prevAffinity = affinityList;
            affinityList.nextAffinity.prevAffinity = this;
            affinityList.nextAffinity = this;
        }

        public void addTo(Node list) {
            assert list.isEdgeNode() : "list is not an edge node";
            assert !this.isEdgeNode() : "this is an edge node";
            assert next == null : "Node already member of list";
            assert prev == null : "Node already member of list";
            next = list.next;
            prev = list;
            list.next.prev = this;
            list.next = this;
        }

        public Node find(Object affinityId) {
            Node n = this.nextAffinity;
            while (!n.isEdgeNode()) {
                if (affinityId.equals(n.pc.affinityId())) {
                    return n;
                }
                n = n.nextAffinity;
            }
            return null;
        }
    }
}
