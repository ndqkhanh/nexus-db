package nexusdb.storage;

import nexusdb.mvcc.VersionChain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A single node in the concurrent B-Tree.
 *
 * Each node holds up to {@code order} keys in sorted order. Leaf nodes store
 * key→VersionChain mappings. Internal nodes store separator keys and child pointers.
 *
 * Concurrency is managed via a per-node ReentrantReadWriteLock, supporting
 * hand-over-hand (crabbing) latch coupling at the tree level.
 */
public final class BTreeNode {

    private final int order; // max keys per node
    private final boolean leaf;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Leaf: keys[i] → values[i] (VersionChain)
    // Internal: children[i] has keys < keys[i], children[keys.size()] has keys >= keys[last]
    private final List<String> keys;
    private final List<VersionChain> values;    // leaf only
    private final List<BTreeNode> children;     // internal only

    private BTreeNode(int order, boolean leaf) {
        this.order = order;
        this.leaf = leaf;
        this.keys = new ArrayList<>();
        this.values = leaf ? new ArrayList<>() : null;
        this.children = leaf ? null : new ArrayList<>();
    }

    public static BTreeNode newLeaf(int order) {
        return new BTreeNode(order, true);
    }

    public static BTreeNode newInternal(int order, BTreeNode left, String separatorKey, BTreeNode right) {
        BTreeNode node = new BTreeNode(order, false);
        node.keys.add(separatorKey);
        node.children.add(left);
        node.children.add(right);
        return node;
    }

    // ========== Query ==========

    public boolean isLeaf() { return leaf; }

    public int keyCount() { return keys.size(); }

    public boolean isFull() { return keys.size() >= order; }

    public String keyAt(int index) { return keys.get(index); }

    /** Leaf search: returns the VersionChain for the key, or null. */
    public VersionChain search(String key) {
        assert leaf : "search() is for leaf nodes only";
        int idx = Collections.binarySearch(keys, key);
        return idx >= 0 ? values.get(idx) : null;
    }

    /** Internal node: find the child that should contain the given key. */
    public BTreeNode findChild(String key) {
        assert !leaf : "findChild() is for internal nodes only";
        int idx = Collections.binarySearch(keys, key);
        if (idx >= 0) {
            // Exact match on separator — go right (keys >= separator go right)
            return children.get(idx + 1);
        } else {
            // insertion point: -(idx) - 1
            int insertionPoint = -idx - 1;
            return children.get(insertionPoint);
        }
    }

    // ========== Mutation ==========

    /** Insert a key-value pair into a leaf node, maintaining sorted order. */
    public void insertEntry(String key, VersionChain chain) {
        assert leaf : "insertEntry() is for leaf nodes only";
        int idx = Collections.binarySearch(keys, key);
        if (idx >= 0) {
            // Key exists — replace
            values.set(idx, chain);
        } else {
            int insertionPoint = -idx - 1;
            keys.add(insertionPoint, key);
            values.add(insertionPoint, chain);
        }
    }

    /** Insert a separator key and right child into an internal node. */
    public void insertChild(String separatorKey, BTreeNode rightChild) {
        assert !leaf : "insertChild() is for internal nodes only";
        int idx = Collections.binarySearch(keys, separatorKey);
        int insertionPoint = idx >= 0 ? idx + 1 : -idx - 1;
        keys.add(insertionPoint, separatorKey);
        children.add(insertionPoint + 1, rightChild);
    }

    /**
     * Get existing VersionChain for key, or create and insert a new one.
     * Used by computeIfAbsent semantics during transaction commit.
     */
    public VersionChain getOrCreate(String key) {
        assert leaf : "getOrCreate() is for leaf nodes only";
        int idx = Collections.binarySearch(keys, key);
        if (idx >= 0) {
            return values.get(idx);
        }
        VersionChain chain = new VersionChain();
        int insertionPoint = -idx - 1;
        keys.add(insertionPoint, key);
        values.add(insertionPoint, chain);
        return chain;
    }

    // ========== Split ==========

    /** Split result: left node, separator key (promoted), right node. */
    public record SplitResult(BTreeNode left, String separatorKey, BTreeNode right) {}

    /**
     * Split this node into two halves.
     *
     * For leaf nodes: keys are divided evenly; separator = first key of right half.
     * For internal nodes: middle key is promoted as separator (not duplicated).
     */
    public SplitResult split() {
        int mid = keys.size() / 2;

        if (leaf) {
            BTreeNode left = new BTreeNode(order, true);
            BTreeNode right = new BTreeNode(order, true);

            for (int i = 0; i < mid; i++) {
                left.keys.add(keys.get(i));
                left.values.add(values.get(i));
            }
            for (int i = mid; i < keys.size(); i++) {
                right.keys.add(keys.get(i));
                right.values.add(values.get(i));
            }

            return new SplitResult(left, right.keys.get(0), right);
        } else {
            // Internal node: promote middle key
            String promotedKey = keys.get(mid);

            BTreeNode left = new BTreeNode(order, false);
            for (int i = 0; i < mid; i++) {
                left.keys.add(keys.get(i));
            }
            for (int i = 0; i <= mid; i++) {
                left.children.add(children.get(i));
            }

            BTreeNode right = new BTreeNode(order, false);
            for (int i = mid + 1; i < keys.size(); i++) {
                right.keys.add(keys.get(i));
            }
            for (int i = mid + 1; i <= children.size() - 1; i++) {
                right.children.add(children.get(i));
            }

            return new SplitResult(left, promotedKey, right);
        }
    }

    // ========== Locking (for hand-over-hand crabbing) ==========

    public void readLock() { rwLock.readLock().lock(); }
    public void readUnlock() { rwLock.readLock().unlock(); }
    public void writeLock() { rwLock.writeLock().lock(); }
    public void writeUnlock() { rwLock.writeLock().unlock(); }

    // ========== Range scan support ==========

    /** Returns all keys in this leaf node (for range scans). */
    List<String> allKeys() {
        assert leaf;
        return Collections.unmodifiableList(keys);
    }

    /** Returns the VersionChain at the given index. */
    VersionChain valueAt(int index) {
        assert leaf;
        return values.get(index);
    }

    /** For internal nodes, returns child count. */
    int childCount() {
        assert !leaf;
        return children.size();
    }

    /** For internal nodes, returns child at index. */
    BTreeNode childAt(int index) {
        assert !leaf;
        return children.get(index);
    }

    /** For internal nodes, replace child at index. Used during split propagation. */
    void replaceChildAt(int index, BTreeNode newChild) {
        assert !leaf;
        children.set(index, newChild);
    }
}
