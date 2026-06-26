package org.qwh.pms.lookup.view;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.Levels;

import java.util.Collections;
import java.util.List;

/** Immutable handle for one bucket's current file view. */
public final class LiveBucketView {

    private final Levels levels;
    private final boolean valid;
    private final long version;

    LiveBucketView(Levels levels, boolean valid, long version) {
        this.levels = levels;
        this.valid = valid;
        this.version = version;
    }

    Levels levels() {
        return levels;
    }

    public boolean valid() {
        return valid;
    }

    public long version() {
        return version;
    }

    public List<DataFileMeta> allFiles() {
        if (!valid) {
            return Collections.emptyList();
        }
        return List.copyOf(levels.allFiles());
    }
}
