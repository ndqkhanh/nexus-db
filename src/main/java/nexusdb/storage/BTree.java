package nexusdb.storage;

import nexusdb.mvcc.VersionChain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Concurrent B-Tree with hand-over-hand (crabbing) latch coupling.
 *
 * Supports point lookups, computeIfAbsent (insert-or-get), and range scans.
 * Thread safety is achieved via per-node ReentrantReadWriteLock with top-down
 * latch coupling:
 *
 * <b>Read path:</b> acquire read latch on parent → acquire read latch on child → release parent.
 * <b>Write path:</b> proactive split-on-descent — split full nodes as we descend,
 *   so the leaf always has room for insertion. This simplifies upward propagation.
 *
 * @see "DDIA Ch3 — B-Trees"
 * @see "Art of Multiprocessor Programming Ch15 — Concurrent Trees"
 */
public final class BTree {

    private final int order;
    private final ReentrantReadWriteLock treeLock = new ReentrantReadWriteLock();
    private volatile BTreeNode root;

    public BTree(int order) {
        this.order = order;
        this.root = BTreeNode.newLeaf(order);
    }

    /**
     * Point lookup: returns the VersionChain for the key, or null.
     * Uses read-latch crabbing (top-down, release parent after acquiring child).
     */
    public VersionChain get(String key) {
        treeLock.readLock().lock();
        BTreeNode current = root;
        current.readLock();
        treeLock.readLock().unlock();

        while (!current.isLeaf()) {
            BTreeNode child = current.findChild(key);
            child.readLock();
            current.readUnlock();
            current = child;
        }

        try {
            return current.search(key);
        } finally {
            current.readUnlock();
        }
    }

    /**
     * Insert-or-get: returns existing VersionChain if key exists,
     * otherwise creates a new one.
     *
     * Uses proactive split-on-descent: any full node encountered during
     * traversal is split immediately, so we never need to propagate splits
     * back up after reaching the leaf.
     */
    public VersionChain computeIfAbsent(String key) {
        treeLock.writeLock().lock();

        // Handle root split
        if (root.isFull()) {
            BTreeNode oldRoot = root;
            BTreeNode.SplitResult split = oldRoot.split();
            root = BTreeNode.newInternal(order, split.left(), split.separatorKey(), split.right());
        }

        BTreeNode current = root;
        current.writeLock();

        while (!current.isLeaf()) {
            BTreeNode child = current.findChild(key);
            child.writeLock();

            if (child.isFull()) {
                // Proactive split: split the full child before descending
                BTreeNode.SplitResult split = child.split();
                current.insertChild(split.separatorKey(), split.right());
                replaceChild(current, child, split.left());
                child.writeUnlock();

                // Re-navigate from current to find the correct (now non-full) child
                BTreeNode newChild = current.findChild(key);
                newChild.writeLock();
                current.writeUnlock();
                current = newChild;
            } else {
                current.writeUnlock();
                current = child;
            }
        }

        // current is a non-full leaf (guaranteed by split-on-descent)
        try {
            VersionChain result = current.getOrCreate(key);
            return result;
        } finally {
            current.writeUnlock();
            treeLock.writeLock().unlock();
        }
    }

    /**
     * Range scan: returns all keys in [fromInclusive, toExclusive) in sorted order.
     * Performs an in-order traversal under the tree read lock.
     */
    public List<String> rangeScanKeys(String fromInclusive, String toExclusive) {
        List<String> result = new ArrayList<>();
        treeLock.readLock().lock();
        try {
            collectInOrder(root, fromInclusive, toExclusive, result);
        } finally {
            treeLock.readLock().unlock();
        }
        return result;
    }

    /**
     * Insert a key with an existing VersionChain (used during recovery).
     * If key already exists, replaces the chain.
     */
    public void put(String key, VersionChain chain) {
        treeLock.writeLock().lock();

        if (root.isFull()) {
            BTreeNode.SplitResult split = root.split();
            root = BTreeNode.newInternal(order, split.left(), split.separatorKey(), split.right());
        }

        BTreeNode current = root;
        current.writeLock();

        while (!current.isLeaf()) {
            BTreeNode child = current.findChild(key);
            child.writeLock();

            if (child.isFull()) {
                BTreeNode.SplitResult split = child.split();
                current.insertChild(split.separatorKey(), split.right());
                replaceChild(current, child, split.left());
                child.writeUnlock();

                BTreeNode newChild = current.findChild(key);
                newChild.writeLock();
                current.writeUnlock();
                current = newChild;
            } else {
                current.writeUnlock();
                current = child;
            }
        }

        try {
            current.insertEntry(key, chain);
        } finally {
            current.writeUnlock();
            treeLock.writeLock().unlock();
        }
    }

    // ========== Internal Helpers ==========

    private void replaceChild(BTreeNode parent, BTreeNode oldChild, BTreeNode newChild) {
        for (int i = 0; i < parent.childCount(); i++) {
            if (parent.childAt(i) == oldChild) {
                parent.replaceChildAt(i, newChild);
                return;
            }
        }
    }

    private void collectInOrder(BTreeNode node, String from, String to, List<String> result) {
        if (node.isLeaf()) {
            for (String key : node.allKeys()) {
                if (key.compareTo(from) >= 0 && key.compareTo(to) < 0) {
                    result.add(key);
                }
            }
        } else {
            for (int i = 0; i < node.childCount(); i++) {
                collectInOrder(node.childAt(i), from, to, result);
            }
        }
    }
}
