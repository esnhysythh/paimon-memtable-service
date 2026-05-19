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

    private final PMSConfig config;
    private final WalConfig walConfig;
    private final Path walDir;
    private final long maxFileSizeBytes;

    private final TreeMap<Long, WalFileInfo> walFiles = new TreeMap<>();
    private LogWriter currentWriter;
    private long nextFileNumber;
    private long currentFileBytes;

    private volatile boolean closed = false;

    public WALManagerImpl(PMSConfig config) {
        this.config = config;
        this.walConfig = new WalConfig(config);
        this.walDir = Path.of(config.walDir());
        this.maxFileSizeBytes = (long) config.walFileSizeMb() * 1024 * 1024;
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

        // Read maxSnapshotId from each existing file's header
        for (WalFileInfo info : walFiles.values()) {
            info.maxSnapshotId = readMaxSnapshotId(info.file);
        }

        // Open current writer (new file if none exist)
        rollToNewFile();
        LOG.info("WALManager initialized, walDir={}, existingFiles={}", walDir, walFiles.size());
    }

    @Override
    public synchronized void appendDataRecord(byte[] key, byte[] value) {
        ensureNotClosed();

        // Serialize: type(1) + keyLen(4) + key + valueLen(4) + [value]
        int payloadSize = 1 + 4 + key.length + 4 + (value != null ? value.length : 0);
        DynamicSliceOutput output = new DynamicSliceOutput(payloadSize);
        output.writeByte(TYPE_DATA);
        output.writeInt(key.length);
        writeBytes(output, key);
        if (value != null) {
            output.writeInt(value.length);
            writeBytes(output, value);
        } else {
            output.writeInt(VALUE_LEN_DELETE);
        }

        addRecord(output.slice(), false);
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
        ensureNotClosed();

        int payloadSize = 1 + 8;
        DynamicSliceOutput output = new DynamicSliceOutput(payloadSize);
        output.writeByte(TYPE_SINK_SUCCESS);
        output.writeLong(snapshotId);

        addRecord(output.slice(), true);

        // Update maxSnapshotId for current file
        WalFileInfo currentInfo = walFiles.get(currentWriter.getFileNumber());
        if (currentInfo != null) {
            currentInfo.maxSnapshotId = Math.max(currentInfo.maxSnapshotId, snapshotId);
        }
    }

    @Override
    public void replay(ReplayCallback callback, long highWatermarkSnapshotId) {
        List<WalFileInfo> filesToReplay;
        synchronized (this) {
            filesToReplay = new ArrayList<>(walFiles.values());
        }

        boolean pastHighWatermark = false;

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
                    byte type = record.getByte(0);
                    switch (type) {
                        case TYPE_DATA -> {
                            SliceInput input = record.input();
                            input.readByte(); // skip type
                            int keyLen = input.readInt();
                            byte[] key = new byte[keyLen];
                            input.readBytes(key);
                            int valueLen = input.readInt();
                            byte[] value;
                            if (valueLen == VALUE_LEN_DELETE) {
                                value = null;
                            } else {
                                value = new byte[valueLen];
                                input.readBytes(value);
                            }
                            callback.onDataRecord(key, value);
                        }
                        case TYPE_SINK_PREPARE -> {
                            SliceInput input = record.input();
                            input.readByte(); // skip type
                            int msgLen = input.readInt();
                            byte[] commitMessage = new byte[msgLen];
                            input.readBytes(commitMessage);
                            callback.onSinkPrepare(commitMessage);
                        }
                        case TYPE_SINK_SUCCESS -> {
                            SliceInput input = record.input();
                            input.readByte(); // skip type
                            long snapshotId = input.readLong();
                            callback.onSinkSuccess(snapshotId);
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

        // Write file header: magic(4) + maxSnapshotId(8) = 12 bytes
        // Magic: "PMS\0"
        DynamicSliceOutput headerOutput = new DynamicSliceOutput(12);
        headerOutput.writeByte('P');
        headerOutput.writeByte('M');
        headerOutput.writeByte('S');
        headerOutput.writeByte(0);
        headerOutput.writeLong(0); // maxSnapshotId = 0 initially

        currentWriter = Logs.createLogWriter(file, fileNum, walConfig);
        currentWriter.addRecord(headerOutput.slice(), true);

        currentFileBytes = estimateRecordSize(12);
        walFiles.put(fileNum, new WalFileInfo(fileNum, file));
    }

    private long readMaxSnapshotId(File file) {
        if (!file.exists() || file.length() == 0) {
            return 0;
        }
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel channel = fis.getChannel()) {
            LogReader reader = new LogReader(channel, LogMonitors.logMonitor(), true, 0);
            Slice headerRecord = reader.readRecord();
            if (headerRecord == null || headerRecord.length() < 12) {
                return 0;
            }
            // Header format: magic(4) + maxSnapshotId(8)
            return headerRecord.getLong(4);
        } catch (IOException e) {
            LOG.warn("Failed to read WAL file header: {}", file, e);
            return 0;
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

        WalFileInfo(long fileNumber, File file) {
            this.fileNumber = fileNumber;
            this.file = file;
            this.maxSnapshotId = 0;
        }
    }
}
