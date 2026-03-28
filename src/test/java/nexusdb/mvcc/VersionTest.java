package nexusdb.mvcc;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

class VersionTest {

    @Test
    @DisplayName("New version starts with ACTIVE status and MAX_VALUE commit timestamp")
    void newVersionIsActive() {
        Version v = new Version(1L, "hello".getBytes(), null);

        assertThat(v.txnId()).isEqualTo(1L);
        assertThat(v.status()).isEqualTo(Version.Status.ACTIVE);
        assertThat(v.commitTimestamp()).isEqualTo(Long.MAX_VALUE);
        assertThat(v.value()).isEqualTo("hello".getBytes());
        assertThat(v.previous()).isNull();
    }

    @Test
    @DisplayName("Version links to previous version in chain")
    void versionLinksToPrevious() {
        Version v1 = new Version(1L, "v1".getBytes(), null);
        Version v2 = new Version(2L, "v2".getBytes(), v1);

        assertThat(v2.previous()).isSameAs(v1);
        assertThat(v1.previous()).isNull();
    }

    @Test
    @DisplayName("Commit sets timestamp and status atomically visible to readers")
    void commitSetsTimestampAndStatus() {
        Version v = new Version(1L, "data".getBytes(), null);

        v.commit(42L);

        assertThat(v.status()).isEqualTo(Version.Status.COMMITTED);
        assertThat(v.commitTimestamp()).isEqualTo(42L);
    }

    @Test
    @DisplayName("Abort sets status to ABORTED, timestamp stays MAX_VALUE")
    void abortSetsStatus() {
        Version v = new Version(1L, "data".getBytes(), null);

        v.abort();

        assertThat(v.status()).isEqualTo(Version.Status.ABORTED);
        assertThat(v.commitTimestamp()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("Null value represents a deletion tombstone")
    void nullValueIsDeletion() {
        Version v = new Version(1L, null, null);

        assertThat(v.value()).isNull();
        assertThat(v.isDeletion()).isTrue();
    }

    @Test
    @DisplayName("Non-null value is not a deletion")
    void nonNullValueIsNotDeletion() {
        Version v = new Version(1L, "data".getBytes(), null);

        assertThat(v.isDeletion()).isFalse();
    }

    @Test
    @DisplayName("isVisibleTo returns true for committed version with commitTs <= snapshotTs")
    void visibleToSnapshotAfterCommit() {
        Version v = new Version(1L, "data".getBytes(), null);
        v.commit(10L);

        assertThat(v.isVisibleTo(10L)).isTrue();
        assertThat(v.isVisibleTo(15L)).isTrue();
    }

    @Test
    @DisplayName("isVisibleTo returns false for committed version with commitTs > snapshotTs")
    void notVisibleToEarlierSnapshot() {
        Version v = new Version(1L, "data".getBytes(), null);
        v.commit(10L);

        assertThat(v.isVisibleTo(9L)).isFalse();
    }

    @Test
    @DisplayName("isVisibleTo returns false for ACTIVE version")
    void activeVersionNotVisible() {
        Version v = new Version(1L, "data".getBytes(), null);

        assertThat(v.isVisibleTo(Long.MAX_VALUE)).isFalse();
    }

    @Test
    @DisplayName("isVisibleTo returns false for ABORTED version")
    void abortedVersionNotVisible() {
        Version v = new Version(1L, "data".getBytes(), null);
        v.abort();

        assertThat(v.isVisibleTo(Long.MAX_VALUE)).isFalse();
    }
}
