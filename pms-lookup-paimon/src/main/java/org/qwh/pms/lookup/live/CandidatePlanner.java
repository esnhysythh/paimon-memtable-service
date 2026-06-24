package org.qwh.pms.lookup.live;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/** Plans candidate data files in the same level order as Paimon's lookup path. */
public class CandidatePlanner {

    private final Comparator<InternalRow> keyComparator;
    private final int startLevel;

    public CandidatePlanner(Comparator<InternalRow> keyComparator, int startLevel) {
        this.keyComparator = keyComparator;
        this.startLevel = startLevel;
    }

    public List<DataFileMeta> plan(LiveBucketView view, InternalRow key) {
        if (!view.valid()) {
            return List.of();
        }
        return plan(view.levels(), key);
    }

    public List<DataFileMeta> plan(Levels levels, InternalRow key) {
        List<DataFileMeta> candidates = new ArrayList<>();
        for (int level = startLevel; level < levels.numberOfLevels(); level++) {
            if (level == 0) {
                addLevel0Candidates(candidates, levels.level0(), key);
            } else {
                DataFileMeta file = candidateInSortedRun(levels.runOfLevel(level), key);
                if (file != null) {
                    candidates.add(file);
                }
            }
        }
        return candidates;
    }

    private void addLevel0Candidates(
            List<DataFileMeta> candidates, TreeSet<DataFileMeta> level0, InternalRow key) {
        for (DataFileMeta file : level0) {
            if (contains(file, key)) {
                candidates.add(file);
            }
        }
    }

    private DataFileMeta candidateInSortedRun(SortedRun level, InternalRow key) {
        if (level.isEmpty()) {
            return null;
        }

        List<DataFileMeta> files = level.files();
        int left = 0;
        int right = files.size() - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (keyComparator.compare(files.get(mid).maxKey(), key) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        DataFileMeta candidate = files.get(right);
        return contains(candidate, key) ? candidate : null;
    }

    private boolean contains(DataFileMeta file, InternalRow key) {
        return keyComparator.compare(file.minKey(), key) <= 0
                && keyComparator.compare(file.maxKey(), key) >= 0;
    }
}
