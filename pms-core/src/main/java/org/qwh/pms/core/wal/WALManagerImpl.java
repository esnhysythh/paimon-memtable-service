package org.qwh.pms.core.wal;

import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.config.WalConfig;
import org.qwh.pms.core.wal.util.DynamicSliceOutput;
import org.qwh.pms.core.wal.util.Slice;
import org.qwh.pms.core.wal.util.SliceInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TreeMap;

public class WALManagerImpl implements WALManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManagerImpl.class);
    private static final byte RECORD_TYPE_DATA_BATCH = 0x42;
    private static final int BATCH_HEADER_SIZE = 1 + 8 + 4;
    static final int VALUE_LEN_DELETE = -1;

    private final WalConfig walConfig;
    private final Path walDir;
    private final long maxFileSizeBytes;
    private final TreeMap<Long, WalFileInfo> walFiles = new TreeMap<>();
    private LogWriter currentWriter;
    private long nextFileNumber;
    private long currentFileBytes;
    private long nextSequenceId = 1;
    private long lastSequenceId;
    private boolean recoveryStarted;
    private boolean closed;

    public WALManagerImpl(PMSConfig config) {
        walConfig = config.wal();
        walDir = Path.of(walConfig.dir());
        maxFileSizeBytes = walConfig.fileSizeBytes();
    }

    /**
     * Recovers once, then opens a writer. The callback must stage results until recovery succeeds.
     * durableSequenceId is the maximum of the validated local flush and Paimon sink boundaries.
     */
    @Override
    public synchronized void recover(long durableSequenceId, ReplayCallback callback) throws IOException {
        if (closed || recoveryStarted) throw new IllegalStateException("WAL recovery already started or closed");
        if (durableSequenceId < 0) throw new IllegalArgumentException("Negative durable sequence boundary");
        Objects.requireNonNull(callback, "callback");
        recoveryStarted = true;
        Files.createDirectories(walDir);
        TreeMap<Long, WalFileInfo> recovered = discoverFiles();
        long recoveredSequence = 0;
        long previousFile = 0;
        long truncateTailTo = -1;
        for (WalFileInfo info : recovered.values()) {
            if (previousFile != 0 && info.fileNumber != previousFile + 1) {
                throw new IOException("WAL file gap before " + info.file + ": missing " + (previousFile + 1));
            }
            try (FileChannel channel = FileChannel.open(info.file.toPath(), StandardOpenOption.READ)) {
                LogReader reader = new LogReader(channel, info.fileNumber == recovered.lastKey());
                long headerSequence = readFileHeader(reader);
                if (previousFile == 0 ? headerSequence > durableSequenceId : headerSequence != recoveredSequence) {
                    throw new IOException("INVALID_FORMAT at offset 0: inconsistent header sequence " + headerSequence);
                }
                recoveredSequence = headerSequence;
                Slice record;
                while ((record = reader.readRecord()) != null) {
                    try {
                        recoveredSequence = replayBatch(record, recoveredSequence, callback);
                    } catch (IOException e) {
                        throw new IOException("record offset " + reader.getLastRecordOffset() + ": " + e.getMessage(), e);
                    }
                }
                info.highWatermark = recoveredSequence;
                if (reader.hasDiscardedTail()) truncateTailTo = reader.getLastRecordEnd();
            } catch (IOException e) {
                throw new IOException("Cannot recover WAL " + info.file + ": " + e.getMessage(), e);
            }
            previousFile = info.fileNumber;
        }
        if (recoveredSequence < durableSequenceId || recoveredSequence == Long.MAX_VALUE) {
            throw new IOException("Inconsistent WAL sequence in " + walDir + ": recovered=" + recoveredSequence
                + ", durable=" + durableSequenceId);
        }
        // Do not treat a file removed during recovery as an obsolete WAL.
        for (WalFileInfo info : recovered.values()) {
            if (!Files.isRegularFile(info.file.toPath(), LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("WAL file missing or replaced during recovery: " + info.file);
            }
        }
        if (truncateTailTo >= 0) {
            File tail = recovered.lastEntry().getValue().file;
            try (FileChannel channel = FileChannel.open(tail.toPath(), StandardOpenOption.WRITE)) {
                channel.truncate(truncateTailTo);
                channel.force(true);
            } catch (IOException e) {
                throw new IOException("Cannot truncate WAL tail " + tail + " at offset " + truncateTailTo, e);
            }
            LOG.warn("Discarded incomplete WAL tail: file={}, completeEnd={}", tail, truncateTailTo);
        }
        lastSequenceId = recoveredSequence;
        nextSequenceId = recoveredSequence + 1;
        nextFileNumber = recovered.isEmpty() ? 1 : recovered.lastKey() + 1;
        walFiles.putAll(recovered);
        rollToNewFile();
        LOG.info("WAL recovered, walDir={}, files={}, lastSequenceId={}", walDir, recovered.size(), lastSequenceId);
    }

    private TreeMap<Long, WalFileInfo> discoverFiles() throws IOException {
        TreeMap<Long, WalFileInfo> files = new TreeMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(walDir, "wal-*")) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                long number;
                try {
                    if (!name.endsWith(".log")) throw new NumberFormatException();
                    number = Long.parseLong(name.substring(4, name.length() - 4));
                    if (number <= 0 || number == Long.MAX_VALUE || !name.equals(fileName(number))) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    throw new IOException("Invalid WAL file name: " + path, e);
                }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Not a regular WAL file: " + path);
                }
                files.put(number, new WalFileInfo(number, path.toFile(), 0));
            }
        }
        return files;
    }

    private static long readFileHeader(LogReader reader) throws IOException {
        Slice header = reader.readRecord();
        if (header == null || header.length() != 20 || reader.getLastRecordOffset() != 0
            || reader.getLastRecordEnd() != 27
            || header.getByte(0) != 'P' || header.getByte(1) != 'M'
            || header.getByte(2) != 'S' || header.getByte(3) != 0
            || header.getLong(4) != 0 || header.getLong(12) < 0) {
            throw new IOException("INVALID_FORMAT at offset 0: invalid PMS WAL header");
        }
        return header.getLong(12);
    }

    @Override
    public synchronized long appendDataRecord(byte[] key, byte[] value) {
        return appendDataRecords(List.of(new DataWrite(key, value)));
    }

    @Override
    public synchronized long appendDataRecords(List<DataWrite> writes) {
        ensureWritable();
        if (writes == null || writes.isEmpty()) throw new IllegalArgumentException("writes must not be empty");
        long sequenceBegin = nextSequenceId;
        long sequenceEnd = Math.addExact(sequenceBegin, writes.size() - 1L);
        long nextSequence = Math.addExact(sequenceEnd, 1);
        long payloadSize = BATCH_HEADER_SIZE;
        for (DataWrite write : writes) {
            if (write == null || write.key() == null) throw new NullPointerException("write and key must not be null");
            payloadSize += 8L + write.key().length + (write.value() == null ? 0 : write.value().length);
        }
        DynamicSliceOutput output = new DynamicSliceOutput(Math.toIntExact(payloadSize));
        output.writeByte(RECORD_TYPE_DATA_BATCH);
        output.writeLong(sequenceBegin);
        output.writeInt(writes.size());
        for (DataWrite write : writes) {
            output.writeInt(write.key().length);
            output.writeBytes(write.key(), 0, write.key().length);
            output.writeInt(write.value() == null ? VALUE_LEN_DELETE : write.value().length);
            if (write.value() != null) output.writeBytes(write.value(), 0, write.value().length);
        }
        try {
            currentWriter.addRecord(output.slice(), false);
        } catch (IOException e) {
            throw new RuntimeException("WAL write failed", e);
        }
        currentFileBytes += estimateRecordSize(output.size());
        lastSequenceId = sequenceEnd;
        nextSequenceId = nextSequence;
        walFiles.get(currentWriter.getFileNumber()).highWatermark = sequenceEnd;
        if (currentFileBytes >= maxFileSizeBytes) {
            try {
                rollToNewFile();
            } catch (IOException e) {
                throw new RuntimeException("WAL roll failed", e);
            }
        }
        return sequenceBegin;
    }

    @Override
    public synchronized long lastSequenceId() {
        return lastSequenceId;
    }

    @Override
    public synchronized void truncate(long safeSequenceId) {
        ensureWritable();
        // A single ordered deleter preserves the suffix, including on deletion failure.
        var iterator = walFiles.values().iterator();
        while (iterator.hasNext()) {
            WalFileInfo info = iterator.next();
            if (info.fileNumber == currentWriter.getFileNumber() || info.highWatermark > safeSequenceId) break;
            try {
                Files.delete(info.file.toPath());
                iterator.remove();
                LOG.info("Truncated WAL file {} (watermark <= {})", info.file, safeSequenceId);
            } catch (IOException e) {
                LOG.warn("Failed to delete WAL file {}; stopping prefix cleanup", info.file, e);
                break;
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
                LOG.warn("Error closing WAL writer", e);
            }
        }
    }

    private void rollToNewFile() throws IOException {
        if (nextFileNumber <= 0 || nextFileNumber == Long.MAX_VALUE) throw new IOException("WAL file numbers exhausted");
        if (currentWriter != null) currentWriter.close();
        long fileNum = nextFileNumber;
        File file = walDir.resolve(fileName(fileNum)).toFile();
        DynamicSliceOutput header = new DynamicSliceOutput(20);
        header.writeByte('P');
        header.writeByte('M');
        header.writeByte('S');
        header.writeByte(0);
        header.writeLong(0);
        header.writeLong(lastSequenceId);
        LogWriter writer = Logs.createLogWriter(file, fileNum, walConfig);
        try {
            writer.addRecord(header.slice(), true);
        } catch (IOException | RuntimeException e) {
            try {
                writer.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        currentWriter = writer;
        nextFileNumber++;
        currentFileBytes = estimateRecordSize(20);
        walFiles.put(fileNum, new WalFileInfo(fileNum, file, lastSequenceId));
    }

    private void ensureWritable() {
        if (closed || currentWriter == null || currentWriter.isClosed()) {
            throw new IllegalStateException("WAL manager is not writable");
        }
    }

    private static String fileName(long number) {
        return String.format(Locale.ROOT, "wal-%06d.log", number);
    }

    private static long estimateRecordSize(int payloadSize) {
        return payloadSize + 7L * (payloadSize / (LogConstants.BLOCK_SIZE - LogConstants.HEADER_SIZE) + 1);
    }

    private static long replayBatch(Slice record, long previousSequence, ReplayCallback callback) throws IOException {
        SliceInput input = record.input();
        requireBytes(input, BATCH_HEADER_SIZE, "DATA batch header");
        if (input.readByte() != RECORD_TYPE_DATA_BATCH) throw new IOException("INVALID_FORMAT: unknown DATA record type");
        long begin = input.readLong();
        int count = input.readInt();
        if (begin <= previousSequence || count <= 0 || count > input.available() / 8
            || begin > Long.MAX_VALUE - count) {
            throw new IOException("INVALID_FORMAT: invalid DATA batch sequence/count");
        }
        // Validate the entire batch before delivering any of it, without allocating row payloads.
        for (int i = 0; i < count; i++) {
            requireBytes(input, 4, "key length");
            int keyLength = input.readInt();
            requireBytes(input, keyLength, "key");
            input.skipBytes(keyLength);
            requireBytes(input, 4, "value length");
            int valueLength = input.readInt();
            if (valueLength != VALUE_LEN_DELETE) {
                requireBytes(input, valueLength, "value");
                input.skipBytes(valueLength);
            }
        }
        if (input.available() != 0) throw new IOException("INVALID_FORMAT: trailing DATA batch bytes");
        input.setPosition(BATCH_HEADER_SIZE);
        for (int i = 0; i < count; i++) {
            byte[] key = input.readByteArray(input.readInt());
            int valueLength = input.readInt();
            byte[] value = valueLength == VALUE_LEN_DELETE ? null : input.readByteArray(valueLength);
            callback.onDataRecord(begin + i, key, value);
        }
        return begin + count - 1;
    }

    private static void requireBytes(SliceInput input, int length, String field) throws IOException {
        if (length < 0 || length > input.available()) {
            throw new IOException("INVALID_FORMAT: invalid " + field + " length " + length);
        }
    }

    private static class WalFileInfo {
        final long fileNumber;
        final File file;
        long highWatermark;

        WalFileInfo(long fileNumber, File file, long highWatermark) {
            this.fileNumber = fileNumber;
            this.file = file;
            this.highWatermark = highWatermark;
        }
    }
}
