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

    private static final byte RECORD_TYPE_DATA_BATCH = 0x42;
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

        // Read maxSequenceId from each existing file.
        for (WalFileInfo info : walFiles.values()) {
            WalFileScan scan = scanWalFile(info.file);
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
        return appendDataRecords(List.of(new DataWrite(key, value)));
    }

    @Override
    public synchronized long appendDataRecords(List<DataWrite> writes) {
        ensureNotClosed();
        if (writes == null || writes.isEmpty()) {
            throw new IllegalArgumentException("writes must not be empty");
        }

        long sequenceBegin = nextSequenceId;
        long sequenceEnd = sequenceBegin + writes.size() - 1;
        nextSequenceId = sequenceEnd + 1;

        // Serialize:
        // recordType(1) + sequenceBegin(8) + count(4)
        // repeated: keyLen(4) + key + valueLen(4) + [value]
        int payloadSize = 1 + 8 + 4;
        for (DataWrite write : writes) {
            if (write == null || write.key() == null) {
                throw new NullPointerException("write and write.key must not be null");
            }
            payloadSize += 4 + write.key().length + 4 + (write.value() != null ? write.value().length : 0);
        }
        DynamicSliceOutput output = new DynamicSliceOutput(payloadSize);
        output.writeByte(RECORD_TYPE_DATA_BATCH);
        output.writeLong(sequenceBegin);
        output.writeInt(writes.size());
        for (DataWrite write : writes) {
            output.writeInt(write.key().length);
            writeBytes(output, write.key());
            if (write.value() != null) {
                output.writeInt(write.value().length);
                writeBytes(output, write.value());
            } else {
                output.writeInt(VALUE_LEN_DELETE);
            }
        }

        long fileNumber = currentWriter.getFileNumber();
        addRecord(output.slice(), false);
        lastSequenceId = sequenceEnd;

        WalFileInfo currentInfo = walFiles.get(fileNumber);
        if (currentInfo != null) {
            currentInfo.observeSequence(sequenceBegin);
            currentInfo.observeSequence(sequenceEnd);
        }
        maybeRollToNewFile();
        return sequenceBegin;
    }

    @Override
    public synchronized long lastSequenceId() {
        return lastSequenceId;
    }

    /**
     * Replay WAL records to the given callback.
     *
     * @param callback receives DATA records in order
     */
    @Override
    public void replay(ReplayCallback callback) {
        List<WalFileInfo> filesToReplay;
        synchronized (this) {
            filesToReplay = new ArrayList<>(walFiles.values());
        }

        for (WalFileInfo info : filesToReplay) {
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
                    replayRecord(record, callback);
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
    public synchronized void truncate(long safeSequenceId) {
        // TODO(pms-wal): detach eligible WAL metadata under this monitor and delete the files
        // outside it, while preserving retry/recovery semantics for failed deletes. The current
        // implementation keeps file deletion simple but can briefly delay concurrent WAL append.
        List<Long> toDelete = new ArrayList<>();
        for (var entry : walFiles.entrySet()) {
            long fileNum = entry.getKey();
            WalFileInfo info = entry.getValue();
            // Never delete the current file
            if (currentWriter != null && fileNum == currentWriter.getFileNumber()) {
                continue;
            }
            // Delete if the file's data records are fully persisted to Paimon.
            if (info.maxSequenceId > 0 && info.maxSequenceId <= safeSequenceId) {
                toDelete.add(fileNum);
            }
        }

        for (Long fileNum : toDelete) {
            WalFileInfo info = walFiles.remove(fileNum);
            if (info != null) {
                try {
                    new LogWriterDeleter(info.file).delete();
                    LOG.info("Truncated WAL file {} (maxSequenceId <= {})", info.file.getName(), safeSequenceId);
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
        } catch (IOException e) {
            throw new RuntimeException("WAL write failed", e);
        }
    }

    private void maybeRollToNewFile() {
        if (currentFileBytes < maxFileSizeBytes) {
            return;
        }
        try {
            rollToNewFile();
        } catch (IOException e) {
            throw new RuntimeException("WAL roll failed", e);
        }
    }

    private synchronized void rollToNewFile() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
        }

        long fileNum = nextFileNumber++;
        File file = walDir.resolve(String.format("wal-%06d.log", fileNum)).toFile();

        // Write file header: magic(4) + reserved(8) + lastSequenceId(8) = 20 bytes
        // Magic: "PMS\0"
        DynamicSliceOutput headerOutput = new DynamicSliceOutput(20);
        headerOutput.writeByte('P');
        headerOutput.writeByte('M');
        headerOutput.writeByte('S');
        headerOutput.writeByte(0);
        headerOutput.writeLong(0); // reserved for future WAL metadata
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
            long headerLastSequenceId = headerRecord.getLong(12);
            long minSequenceId = Long.MAX_VALUE;
            long maxSequenceId = headerLastSequenceId;

                Slice record;
                while ((record = reader.readRecord()) != null) {
                    SequenceRange range = sequenceRange(record);
                    if (range.maxSequenceId() > 0) {
                        minSequenceId = Math.min(minSequenceId, range.minSequenceId());
                        maxSequenceId = Math.max(maxSequenceId, range.maxSequenceId());
                    }
                }
            return new WalFileScan(minSequenceId == Long.MAX_VALUE ? 0 : minSequenceId, maxSequenceId);
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

    private static void replayRecord(Slice record, ReplayCallback callback) {
        if (record.length() > 0 && record.getByte(0) == RECORD_TYPE_DATA_BATCH) {
            replayBatchRecord(record, callback);
        } else {
            replayLegacyDataRecord(record, callback);
        }
    }

    private static void replayBatchRecord(Slice record, ReplayCallback callback) {
        SliceInput input = record.input();
        input.readByte();
        requireBytes(input, 8 + 4, "DATA batch header");
        long sequenceBegin = input.readLong();
        if (sequenceBegin <= 0) {
            throw corruptRecord("Invalid batch sequenceBegin: " + sequenceBegin);
        }
        int count = input.readInt();
        if (count <= 0) {
            throw corruptRecord("Invalid DATA batch count: " + count);
        }
        for (int i = 0; i < count; i++) {
            DataRecordPayload payload = readDataRecordPayload(input);
            callback.onDataRecord(sequenceBegin + i, payload.key(), payload.value());
        }
        requireFullyConsumed(input, "DATA batch");
    }

    private static void replayLegacyDataRecord(Slice record, ReplayCallback callback) {
        SliceInput input = record.input();
        requireBytes(input, 8 + 4, "DATA header");
        long sequenceId = input.readLong();
        if (sequenceId <= 0) {
            throw corruptRecord("Invalid sequenceId: " + sequenceId);
        }
        DataRecordPayload payload = readDataRecordPayload(input);
        requireFullyConsumed(input, "DATA");
        callback.onDataRecord(sequenceId, payload.key(), payload.value());
    }

    private static SequenceRange sequenceRange(Slice record) {
        if (record.length() > 0 && record.getByte(0) == RECORD_TYPE_DATA_BATCH) {
            SliceInput input = record.input();
            input.readByte();
            requireBytes(input, 8 + 4, "DATA batch header");
            long sequenceBegin = input.readLong();
            int count = input.readInt();
            if (sequenceBegin <= 0 || count <= 0) {
                return SequenceRange.EMPTY;
            }
            return new SequenceRange(sequenceBegin, sequenceBegin + count - 1);
        }
        if (record.length() < 8) {
            return SequenceRange.EMPTY;
        }
        long sequenceId = record.getLong(0);
        return sequenceId > 0 ? new SequenceRange(sequenceId, sequenceId) : SequenceRange.EMPTY;
    }

    private static DataRecordPayload readDataRecordPayload(SliceInput input) {
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
        return new DataRecordPayload(key, value);
    }

    private record DataRecordPayload(byte[] key, byte[] value) {}

    private record SequenceRange(long minSequenceId, long maxSequenceId) {
        private static final SequenceRange EMPTY = new SequenceRange(0, 0);
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
        long minSequenceId;
        long maxSequenceId;

        WalFileInfo(long fileNumber, File file) {
            this.fileNumber = fileNumber;
            this.file = file;
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

    private record WalFileScan(long minSequenceId, long maxSequenceId) {
        static final WalFileScan EMPTY = new WalFileScan(0, 0);
    }
}
