package org.qwh.pms.core.wal;

import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.wal.util.DynamicSliceOutput;
import org.qwh.pms.core.wal.util.Slice;
import org.qwh.pms.core.wal.util.SliceInput;
import org.qwh.pms.core.wal.util.Slices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class WALManagerImpl implements WALManager {

    private static final Logger LOG = LoggerFactory.getLogger(WALManagerImpl.class);

    // PMS application-layer record types
    static final byte TYPE_DATA = 0x00;
    static final byte TYPE_SINK_PREPARE = 0x01;
    static final byte TYPE_SINK_SUCCESS = 0x02;

    // valueLen = -1 signals Delete (no value bytes follow)
    static final int VALUE_LEN_DELETE = -1;

    private final org.qwh.pms.core.config.WalConfig walConfig;
    private final Path walDir;
    private final long maxFileSizeBytes;

    private final TreeMap<Long, WalFileInfo> walFiles = new TreeMap<>();
    private LogWriter currentWriter;
    private long nextFileNumber;
    private long currentFileBytes;
    private long nextSequenceId = 1;
    private long lastSequenceId = 0;

    private volatile boolean closed = false;

    public WALManagerImpl(PMSConfig config) {
        this.walConfig = config.wal();
        this.walDir = Path.of(config.wal().dir());
        this.maxFileSizeBytes = config.wal().fileSizeBytes();
    }

    /**
     * Initialize WAL manager: create directory, discover existing WAL files, open writer.
     * Must be called after construction before any other operations.
     */
    public synchronized void init() throws IOException {
        Files.createDirectories(walDir);

        // Discover existing WAL files and determine next file number
        nextFileNumber = 1;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(walDir, "wal-*.log")) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                long fileNum = parseFileNumber(name);
                if (fileNum > 0) {
                    walFiles.put(fileNum, new WalFileInfo(fileNum, path.toFile()));
                    nextFileNumber = Math.max(nextFileNumber, fileNum + 1);
                }
            }
        }

        // Read maxSnapshotId and maxSequenceId from each existing file.
        for (WalFileInfo info : walFiles.values()) {
            WalFileScan scan = scanWalFile(info.file);
            info.maxSnapshotId = scan.maxSnapshotId;
            info.minSequenceId = scan.minSequenceId;
            info.maxSequenceId = scan.maxSequenceId;
            lastSequenceId = Math.max(lastSequenceId, scan.maxSequenceId);
        }
        nextSequenceId = lastSequenceId + 1;

        // Open current writer (new file if none exist)
        rollToNewFile();
        LOG.info("WALManager initialized, walDir={}, existingFiles={}", walDir, walFiles.size());
    }

    @Override
    public synchronized long appendDataRecord(byte[] key, byte[] value) {
        ensureNotClosed();

        long sequenceId = nextSequenceId++;

        // Serialize: type(1) + sequenceId(8) + keyLen(4) + key + valueLen(4) + [value]
        int payloadSize = 1 + 8 + 4 + key.length + 4 + (value != null ? value.length : 0);
        DynamicSliceOutput output = new DynamicSliceOutput(payloadSize);
        output.writeByte(TYPE_DATA);
        output.writeLong(sequenceId);
        output.writeInt(key.length);
        writeBytes(output, key);
        if (value != null) {
            output.writeInt(value.length);
            writeBytes(output, value);
        } else {
            output.writeInt(VALUE_LEN_DELETE);
        }

        long fileNumber = currentWriter.getFileNumber();
        addRecord(output.slice(), false);
        lastSequenceId = sequenceId;

        WalFileInfo currentInfo = walFiles.get(fileNumber);
        if (currentInfo != null) {
            currentInfo.observeSequence(sequenceId);
        }
        return sequenceId;
    }

    @Override
    public synchronized long lastSequenceId() {
        return lastSequenceId;
    }

    @Override
    public synchronized void appendSinkPrepare(byte[] commitMessage) {
        ensureNotClosed();

        int payloadSize = 1 + 4 + commitMessage.length;
        DynamicSliceOutput output = new DynamicSliceOutput(payloadSize);
        output.writeByte(TYPE_SINK_PREPARE);
        output.writeInt(commitMessage.length);
        writeBytes(output, commitMessage);

        addRecord(output.slice(), true);
    }

    @Override
    public synchronized void appendSinkSuccess(long snapshotId) {
        appendSinkSuccess(snapshotId, new byte[0]);
    }

    @Override
    public synchronized void appendSinkSuccess(long snapshotId, byte[] metadata) {
        ensureNotClosed();
        if (metadata == null) {
            metadata = new byte[0];
        }

        int payloadSize = 1 + 8 + 4 + metadata.length;
        DynamicSliceOutput output = new DynamicSliceOutput(payloadSize);
        output.writeByte(TYPE_SINK_SUCCESS);
        output.writeLong(snapshotId);
        output.writeInt(metadata.length);
        writeBytes(output, metadata);

        long fileNumber = currentWriter.getFileNumber();
        addRecord(output.slice(), true);

        // Update maxSnapshotId for current file
        WalFileInfo currentInfo = walFiles.get(fileNumber);
        if (currentInfo != null) {
            currentInfo.maxSnapshotId = Math.max(currentInfo.maxSnapshotId, snapshotId);
        }
    }

    /**
     * Replay WAL records to the given callback.
     *
     * @param callback receives DATA, SINK_PREPARE, and SINK_SUCCESS records in order
     * @param highWatermarkSnapshotId controls which DATA records are skipped:
     *   &lt;= 0 or {@code Long.MAX_VALUE} — replay all DATA records (no filtering);
     *   positive value — skip DATA records before the first SINK_SUCCESS whose
     *   snapshotId &gt;= this value (those records are already committed to Paimon).
     *   Control records (SINK_PREPARE / SINK_SUCCESS) are always replayed regardless
     *   of the watermark, so the caller can track the full snapshot timeline.
     */
    @Override
    public void replay(ReplayCallback callback, long highWatermarkSnapshotId) {
        List<WalFileInfo> filesToReplay;
        synchronized (this) {
            filesToReplay = new ArrayList<>(walFiles.values());
        }

        // Whether we've encountered a SINK_SUCCESS with snapshotId >= highWatermarkSnapshotId.
        // Before this point, DATA records are from already-committed snapshots and can be skipped.
        // Control records (SINK_PREPARE/SINK_SUCCESS) are always replayed so the caller can
        // track the full snapshot timeline.
        // highWatermarkSnapshotId = Long.MAX_VALUE means "nothing is committed yet, replay all".
        boolean pastHighWatermark = highWatermarkSnapshotId <= 0 || highWatermarkSnapshotId == Long.MAX_VALUE;

        for (WalFileInfo info : filesToReplay) {
            // Skip entire files whose maxSnapshotId is strictly before the watermark.
            // A file with maxSnapshotId == highWatermarkSnapshotId can still contain
            // uncommitted DATA records after that SINK_SUCCESS, so it must be scanned.
            if (!pastHighWatermark && info.maxSnapshotId > 0 && info.maxSnapshotId < highWatermarkSnapshotId) {
                continue;
            }

            if (!info.file.exists()) {
                LOG.warn("WAL file missing during replay: {}", info.file);
                continue;
            }

            try (FileInputStream fis = new FileInputStream(info.file);
                 FileChannel channel = fis.getChannel()) {

                if (channel.size() == 0) {
                    continue;
                }

                LogReader reader = new LogReader(
                        channel,
                        LogMonitors.logMonitor(),
                        true,  // verify checksums
                        0      // start from beginning
                );

                // First record is the file header — skip it
                Slice headerRecord = reader.readRecord();
                if (headerRecord == null || headerRecord.length() < 4) {
                    continue;
                }

                Slice record;
                while ((record = reader.readRecord()) != null) {
                    byte type = record.getByte(0);
                    switch (type) {
                        case TYPE_DATA -> {
                            if (!pastHighWatermark) {
                                // Data before highWatermark is already committed to Paimon — skip
                                continue;
                            }
                            SliceInput input = record.input();
                            requireBytes(input, 1 + 8 + 4, "DATA header");
                            input.readByte(); // skip type
                            long sequenceId = input.readLong();
                            if (sequenceId <= 0) {
                                throw corruptRecord("Invalid sequenceId: " + sequenceId);
                            }
                            int keyLen = readNonNegativeLength(input, "keyLen");
                            requireBytes(input, keyLen + 4, "DATA key/value header");
                            byte[] key = new byte[keyLen];
                            input.readBytes(key);
                            int valueLen = input.readInt();
                            byte[] value;
                            if (valueLen == VALUE_LEN_DELETE) {
                                value = null;
                            } else {
                                if (valueLen < 0) {
                                    throw corruptRecord("Invalid valueLen: " + valueLen);
                                }
                                requireBytes(input, valueLen, "DATA value");
                                value = new byte[valueLen];
                                input.readBytes(value);
                            }
                            requireFullyConsumed(input, "DATA");
                            callback.onDataRecord(sequenceId, key, value);
                        }
                        case TYPE_SINK_PREPARE -> {
                            SliceInput input = record.input();
                            requireBytes(input, 1 + 4, "SINK_PREPARE header");
                            input.readByte(); // skip type
                            int msgLen = readNonNegativeLength(input, "commitMessageLen");
                            requireBytes(input, msgLen, "SINK_PREPARE message");
                            byte[] commitMessage = new byte[msgLen];
                            input.readBytes(commitMessage);
                            requireFullyConsumed(input, "SINK_PREPARE");
                            callback.onSinkPrepare(commitMessage);
                        }
                        case TYPE_SINK_SUCCESS -> {
                            SliceInput input = record.input();
                            requireBytes(input, 1 + 8, "SINK_SUCCESS payload");
                            input.readByte(); // skip type
                            long snapshotId = input.readLong();
                            byte[] metadata = new byte[0];
                            if (input.available() > 0) {
                                requireBytes(input, 4, "SINK_SUCCESS metadata length");
                                int metadataLen = readNonNegativeLength(input, "metadataLen");
                                requireBytes(input, metadataLen, "SINK_SUCCESS metadata");
                                metadata = new byte[metadataLen];
                                input.readBytes(metadata);
                            }
                            requireFullyConsumed(input, "SINK_SUCCESS");
                            callback.onSinkSuccess(snapshotId, metadata);
                            if (snapshotId >= highWatermarkSnapshotId) {
                                pastHighWatermark = true;
                            }
                        }
                        default -> LOG.warn("Unknown PMS record type {} during replay", type);
                    }
                }
            } catch (IOException e) {
                LOG.error("Error reading WAL file during replay: {}", info.file, e);
                throw new RuntimeException("WAL replay failed for file: " + info.file, e);
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                LOG.error("Corrupt WAL record during replay: {}", info.file, e);
                throw new RuntimeException("Corrupt WAL record in file: " + info.file, e);
            }
        }
    }

    @Override
    public synchronized void truncate(long safeSnapshotId) {
        List<Long> toDelete = new ArrayList<>();
        for (var entry : walFiles.entrySet()) {
            long fileNum = entry.getKey();
            WalFileInfo info = entry.getValue();
            // Never delete the current file
            if (currentWriter != null && fileNum == currentWriter.getFileNumber()) {
                continue;
            }
            // Delete if maxSnapshotId <= safeSnapshotId
            if (info.maxSnapshotId > 0 && info.maxSnapshotId <= safeSnapshotId) {
                toDelete.add(fileNum);
            }
        }

        for (Long fileNum : toDelete) {
            WalFileInfo info = walFiles.remove(fileNum);
            if (info != null) {
                try {
                    new LogWriterDeleter(info.file).delete();
                    LOG.info("Truncated WAL file {} (snapshotId <= {})", info.file.getName(), safeSnapshotId);
                } catch (IOException e) {
                    LOG.warn("Failed to delete WAL file {}: {}", info.file, e.getMessage());
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;

        if (currentWriter != null) {
            try {
                currentWriter.close();
            } catch (IOException e) {
                LOG.warn("Error closing WAL writer: {}", e.getMessage());
            }
        }
    }

    // ── Internal methods ──

    private void addRecord(Slice payload, boolean force) {
        try {
            currentWriter.addRecord(payload, force);
            currentFileBytes += estimateRecordSize(payload.length());

            // Roll to new file if size exceeds limit
            if (currentFileBytes >= maxFileSizeBytes) {
                rollToNewFile();
            }
        } catch (IOException e) {
            throw new RuntimeException("WAL write failed", e);
        }
    }

    private synchronized void rollToNewFile() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
        }

        long fileNum = nextFileNumber++;
        File file = walDir.resolve(String.format("wal-%06d.log", fileNum)).toFile();

        // Write file header: magic(4) + maxSnapshotId(8) + lastSequenceId(8) = 20 bytes
        // Magic: "PMS\0"
        DynamicSliceOutput headerOutput = new DynamicSliceOutput(20);
        headerOutput.writeByte('P');
        headerOutput.writeByte('M');
        headerOutput.writeByte('S');
        headerOutput.writeByte(0);
        headerOutput.writeLong(0); // maxSnapshotId = 0 initially
        headerOutput.writeLong(lastSequenceId);

        currentWriter = Logs.createLogWriter(file, fileNum, walConfig);
        currentWriter.addRecord(headerOutput.slice(), true);

        currentFileBytes = estimateRecordSize(20);
        walFiles.put(fileNum, new WalFileInfo(fileNum, file));
    }

    private WalFileScan scanWalFile(File file) {
        if (!file.exists() || file.length() == 0) {
            return WalFileScan.EMPTY;
        }
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel channel = fis.getChannel()) {
            LogReader reader = new LogReader(channel, LogMonitors.logMonitor(), true, 0);

            // Skip file header record
            Slice headerRecord = reader.readRecord();
            if (headerRecord == null || headerRecord.length() < 20) {
                return WalFileScan.EMPTY;
            }
            // Read header's initial maxSnapshotId
            long maxSnapshotId = headerRecord.getLong(4);
            long headerLastSequenceId = headerRecord.getLong(12);
            long minSequenceId = Long.MAX_VALUE;
            long maxSequenceId = headerLastSequenceId;

            // Scan all records to find the true maxSnapshotId from SINK_SUCCESS entries.
            // The header only stores the initial value (0); actual snapshotIds come from
            // SINK_SUCCESS records written during the file's lifetime, which are not
            // persisted back to the header.
            Slice record;
            while ((record = reader.readRecord()) != null) {
                if (record.length() < 1) continue;
                byte type = record.getByte(0);
                if (type == TYPE_SINK_SUCCESS && record.length() >= 9) {
                    long snapshotId = record.getLong(1);
                    maxSnapshotId = Math.max(maxSnapshotId, snapshotId);
                } else if (type == TYPE_DATA && record.length() >= 9) {
                    long sequenceId = record.getLong(1);
                    if (sequenceId > 0) {
                        minSequenceId = Math.min(minSequenceId, sequenceId);
                        maxSequenceId = Math.max(maxSequenceId, sequenceId);
                    }
                }
            }
            return new WalFileScan(maxSnapshotId, minSequenceId == Long.MAX_VALUE ? 0 : minSequenceId, maxSequenceId);
        } catch (IOException e) {
            LOG.warn("Failed to scan WAL file metadata: {}", file, e);
            return WalFileScan.EMPTY;
        }
    }

    private long parseFileNumber(String fileName) {
        // Format: wal-000001.log
        if (!fileName.startsWith("wal-") || !fileName.endsWith(".log")) {
            return -1;
        }
        try {
            String numPart = fileName.substring(4, fileName.length() - 4);
            return Long.parseLong(numPart);
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            return -1;
        }
    }

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("WALManager is closed");
    }

    private static void writeBytes(DynamicSliceOutput output, byte[] data) {
        output.writeBytes(data, 0, data.length);
    }

    private static int readNonNegativeLength(SliceInput input, String fieldName) {
        int length = input.readInt();
        if (length < 0) {
            throw corruptRecord("Invalid " + fieldName + ": " + length);
        }
        return length;
    }

    private static void requireBytes(SliceInput input, int length, String fieldName) {
        if (length < 0 || input.available() < length) {
            throw corruptRecord("Truncated " + fieldName + ", required=" + length + ", available=" + input.available());
        }
    }

    private static void requireFullyConsumed(SliceInput input, String recordType) {
        if (input.available() != 0) {
            throw corruptRecord(recordType + " record has trailing bytes: " + input.available());
        }
    }

    private static IllegalArgumentException corruptRecord(String message) {
        return new IllegalArgumentException(message);
    }

    private static int estimateRecordSize(int payloadSize) {
        // LevelDB overhead: header per chunk (7 bytes) + block alignment padding
        // Rough estimate: payload + 7 bytes per 32KB block
        return payloadSize + 7 * ((payloadSize / (LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE)) + 1);
    }

    /**
     * Helper to delete a WAL file via LogWriter.delete() when we have the file but not the writer.
     */
    private static class LogWriterDeleter {
        private final File file;

        LogWriterDeleter(File file) {
            this.file = file;
        }

        void delete() throws IOException {
            if (!file.delete()) {
                throw new IOException("Failed to delete WAL file: " + file);
            }
        }
    }

    private static class WalFileInfo {
        final long fileNumber;
        final File file;
        long maxSnapshotId;
        long minSequenceId;
        long maxSequenceId;

        WalFileInfo(long fileNumber, File file) {
            this.fileNumber = fileNumber;
            this.file = file;
            this.maxSnapshotId = 0;
            this.minSequenceId = 0;
            this.maxSequenceId = 0;
        }

        void observeSequence(long sequenceId) {
            if (sequenceId <= 0) {
                return;
            }
            if (minSequenceId == 0 || sequenceId < minSequenceId) {
                minSequenceId = sequenceId;
            }
            maxSequenceId = Math.max(maxSequenceId, sequenceId);
        }
    }

    private record WalFileScan(long maxSnapshotId, long minSequenceId, long maxSequenceId) {
        static final WalFileScan EMPTY = new WalFileScan(0, 0, 0);
    }
}
