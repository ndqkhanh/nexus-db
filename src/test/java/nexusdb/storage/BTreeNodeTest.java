package nexusdb.storage;

import nexusdb.mvcc.VersionChain;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for BTreeNode — the building block of the concurrent B-Tree.
 *
 * Tests cover: search, insert, split, key ordering, leaf vs internal nodes,
 * and capacity boundaries.
 */
class BTreeNodeTest {

    private static final int ORDER = 4; // max keys per node

    @Test
    void newLeafNodeIsEmpty() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        assertThat(node.isLeaf()).isTrue();
        assertThat(node.keyCount()).isEqualTo(0);
        assertThat(node.isFull()).isFalse();
    }

    @Test
    void insertIntoLeafMaintainsSortedOrder() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        VersionChain c1 = new VersionChain();
        VersionChain c2 = new VersionChain();
        VersionChain c3 = new VersionChain();

        node.insertEntry("charlie", c1);
        node.insertEntry("alice", c2);
        node.insertEntry("bob", c3);

        assertThat(node.keyCount()).isEqualTo(3);
        assertThat(node.keyAt(0)).isEqualTo("alice");
        assertThat(node.keyAt(1)).isEqualTo("bob");
        assertThat(node.keyAt(2)).isEqualTo("charlie");
    }

    @Test
    void searchFindsInsertedKey() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        VersionChain chain = new VersionChain();
        node.insertEntry("key1", chain);

        assertThat(node.search("key1")).isSameAs(chain);
    }

    @Test
    void searchReturnsNullForMissingKey() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        node.insertEntry("key1", new VersionChain());

        assertThat(node.search("missing")).isNull();
    }

    @Test
    void nodeReportsFullWhenAtCapacity() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        for (int i = 0; i < ORDER; i++) {
            node.insertEntry("key" + i, new VersionChain());
        }
        assertThat(node.isFull()).isTrue();
        assertThat(node.keyCount()).isEqualTo(ORDER);
    }

    @Test
    void splitLeafProducesTwoHalves() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        VersionChain[] chains = new VersionChain[ORDER];
        // Insert keys: a, b, c, d (ORDER=4)
        String[] keys = {"delta", "bravo", "alpha", "charlie"};
        for (int i = 0; i < ORDER; i++) {
            chains[i] = new VersionChain();
            node.insertEntry(keys[i], chains[i]);
        }
        // Node should be full and sorted: alpha, bravo, charlie, delta
        assertThat(node.isFull()).isTrue();

        BTreeNode.SplitResult split = node.split();

        // Left keeps lower half, right gets upper half
        assertThat(split.left().keyCount()).isGreaterThanOrEqualTo(ORDER / 2);
        assertThat(split.right().keyCount()).isGreaterThanOrEqualTo(ORDER / 2);
        assertThat(split.left().keyCount() + split.right().keyCount()).isEqualTo(ORDER);

        // Separator key is the first key of the right node
        assertThat(split.separatorKey()).isEqualTo(split.right().keyAt(0));

        // All keys preserved across the split
        assertThat(split.left().keyAt(0)).isEqualTo("alpha");
        assertThat(split.left().keyAt(1)).isEqualTo("bravo");
        assertThat(split.right().keyAt(0)).isEqualTo("charlie");
        assertThat(split.right().keyAt(1)).isEqualTo("delta");
    }

    @Test
    void internalNodeRoutesToCorrectChild() {
        // Create internal node with separator "m"
        // Left child has keys < "m", right child has keys >= "m"
        BTreeNode left = BTreeNode.newLeaf(ORDER);
        left.insertEntry("alpha", new VersionChain());

        BTreeNode right = BTreeNode.newLeaf(ORDER);
        right.insertEntry("zulu", new VersionChain());

        BTreeNode internal = BTreeNode.newInternal(ORDER, left, "mike", right);

        assertThat(internal.isLeaf()).isFalse();
        assertThat(internal.keyCount()).isEqualTo(1);
        assertThat(internal.findChild("alpha")).isSameAs(left);
        assertThat(internal.findChild("delta")).isSameAs(left);
        assertThat(internal.findChild("mike")).isSameAs(right);
        assertThat(internal.findChild("zulu")).isSameAs(right);
    }

    @Test
    void internalNodeWithMultipleSeparators() {
        BTreeNode c0 = BTreeNode.newLeaf(ORDER);
        BTreeNode c1 = BTreeNode.newLeaf(ORDER);
        BTreeNode c2 = BTreeNode.newLeaf(ORDER);

        BTreeNode internal = BTreeNode.newInternal(ORDER, c0, "delta", c1);
        internal.insertChild("mike", c2);

        // Keys < "delta" → c0, "delta" <= keys < "mike" → c1, keys >= "mike" → c2
        assertThat(internal.findChild("alpha")).isSameAs(c0);
        assertThat(internal.findChild("delta")).isSameAs(c1);
        assertThat(internal.findChild("foxtrot")).isSameAs(c1);
        assertThat(internal.findChild("mike")).isSameAs(c2);
        assertThat(internal.findChild("zulu")).isSameAs(c2);
    }

    @Test
    void splitInternalNodeDistributesChildrenCorrectly() {
        // Build an internal node at capacity, then split
        BTreeNode[] children = new BTreeNode[ORDER + 1];
        for (int i = 0; i <= ORDER; i++) {
            children[i] = BTreeNode.newLeaf(ORDER);
        }

        BTreeNode internal = BTreeNode.newInternal(ORDER, children[0], "b", children[1]);
        internal.insertChild("d", children[2]);
        internal.insertChild("f", children[3]);
        internal.insertChild("h", children[4]);

        assertThat(internal.isFull()).isTrue();

        BTreeNode.SplitResult split = internal.split();

        // Total keys across both halves + promoted separator = ORDER
        int totalKeys = split.left().keyCount() + split.right().keyCount() + 1; // +1 for promoted key
        assertThat(totalKeys).isEqualTo(ORDER);

        // Separator is promoted (not duplicated in either child)
        assertThat(split.separatorKey()).isNotNull();
    }

    @Test
    void rwLockSupportsReadAndWriteAccess() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);

        // Acquire read lock
        node.readLock();
        // Can still read
        assertThat(node.keyCount()).isEqualTo(0);
        node.readUnlock();

        // Acquire write lock
        node.writeLock();
        node.insertEntry("key", new VersionChain());
        assertThat(node.keyCount()).isEqualTo(1);
        node.writeUnlock();
    }

    @Test
    void getOrCreateReturnsExistingChain() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);
        VersionChain original = new VersionChain();
        node.insertEntry("key", original);

        VersionChain found = node.getOrCreate("key");
        assertThat(found).isSameAs(original);
    }

    @Test
    void getOrCreateInsertsNewChainIfAbsent() {
        BTreeNode node = BTreeNode.newLeaf(ORDER);

        VersionChain created = node.getOrCreate("newkey");
        assertThat(created).isNotNull();
        assertThat(node.keyCount()).isEqualTo(1);
        assertThat(node.search("newkey")).isSameAs(created);
    }
}
