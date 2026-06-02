package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

final class SSTWriter {
    private final Path dir;
    private final int blockSize;
    private final int restartInterval;

    SSTWriter(Path dir, int blockSize, int restartInterval) {
        this.dir = dir;
        this.blockSize = blockSize;
        this.restartInterval = restartInterval;
    }

    SSTMeta write(long runId, long flushId, ImmutableMemTable memTable) throws IOException {
        return write(
            runId,
            flushId,
            flushId,
            SSTState.NEW,
            memTable.iterator(),
            Math.max(1, memTable.estimatedEntryCount()),
            memTable.minSequenceId(),
            memTable.maxSequenceId()
        );
    }

    SSTMeta write(long runId, long minFlushId, long maxFlushId, SSTState state, Iterator<Entry> entries,
                  long expectedEntryCount, long expectedMinSequenceId, long expectedMaxSequenceId)
            throws IOException {
        Files.createDirectories(dir);
        Path target = pathFor(minFlushId, maxFlushId, state);
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp-" + runId);
        Files.deleteIfExists(tmp);

        List<BlockReaders.IndexEntry> indexEntries = new ArrayList<>();
        BloomFilter bloom = BloomFilter.create((int) Math.min(Integer.MAX_VALUE, Math.max(1, expectedEntryCount)),
            SSTFormat.DEFAULT_BLOOM_FPP);
        DataBlockBuilder blockBuilder = new DataBlockBuilder(restartInterval);
        Key minKey = null;
        Key maxKey = null;
        boolean hasTombstone = false;
        long entryCount = 0;
        long dataBlockCount = 0;
        long minSequenceId = 0;
        long maxSequenceId = 0;

        try (CountingCrcOutputStream out = new CountingCrcOutputStream(new FileOutputStream(tmp.toFile()))) {
            while (entries.hasNext()) {
                Entry entry = entries.next();
                if (minKey == null) {
                    minKey = entry.key();
                }
                maxKey = entry.key();
                hasTombstone |= entry.value().isTombstone();
                long sequenceId = entry.value().sequenceId();
                minSequenceId = minSequenceId == 0 ? sequenceId : Math.min(minSequenceId, sequenceId);
                maxSequenceId = Math.max(maxSequenceId, sequenceId);
                blockBuilder.add(entry);
                bloom = bloom.add(entry.key());
                entryCount++;

                if (blockBuilder.estimatedSize() >= blockSize) {
                    dataBlockCount++;
                    writeDataBlock(out, blockBuilder, indexEntries);
                    blockBuilder = new DataBlockBuilder(restartInterval);
                }
            }
            if (!blockBuilder.isEmpty()) {
                dataBlockCount++;
                writeDataBlock(out, blockBuilder, indexEntries);
            }

            BlockHandle bloomHandle = writeBlock(out, bloom.encode());
            BlockHandle indexHandle = writeIndexBlock(out, indexEntries);
            long createdAtMillis = System.currentTimeMillis();
            byte[] properties = encodeProperties(
                entryCount,
                dataBlockCount,
                minKey,
                maxKey,
                entryCount == 0 ? expectedMinSequenceId : minSequenceId,
                entryCount == 0 ? expectedMaxSequenceId : maxSequenceId,
                createdAtMillis,
                hasTombstone
            );
            BlockHandle propertiesHandle = writeBlock(out, properties);
            long fileSize = out.position() + SSTFormat.FOOTER_SIZE;
            byte[] footer = encodeFooter(bloomHandle, indexHandle, propertiesHandle, (int) out.crcValue());
            out.writeFooter(footer);
            out.flush();
            out.force();

            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            StorageFiles.forceDirectory(dir);
            return new SSTMeta(
                runId,
                minFlushId,
                maxFlushId,
                target,
                fileSize,
                entryCount,
                minKey,
                maxKey,
                entryCount == 0 ? expectedMinSequenceId : minSequenceId,
                entryCount == 0 ? expectedMaxSequenceId : maxSequenceId,
                createdAtMillis,
                state,
                0
            );
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
    }

    static Path pathFor(Path dir, long minFlushId, long maxFlushId, SSTState state) {
        String label = state == SSTState.SINKED ? "sinked" : "new";
        return dir.resolve(String.format("sst-%06d-%06d.%s.sst", minFlushId, maxFlushId, label));
    }

    private Path pathFor(long minFlushId, long maxFlushId, SSTState state) {
        return pathFor(dir, minFlushId, maxFlushId, state);
    }

    private static void writeDataBlock(CountingCrcOutputStream out, DataBlockBuilder blockBuilder,
                                       List<BlockReaders.IndexEntry> indexEntries) throws IOException {
        BlockHandle handle = writeBlock(out, blockBuilder.finish());
        indexEntries.add(new BlockReaders.IndexEntry(blockBuilder.lastKey(), handle));
    }

    private static BlockHandle writeIndexBlock(CountingCrcOutputStream out, List<BlockReaders.IndexEntry> entries)
            throws IOException {
        KeyValueBlockBuilder builder = new KeyValueBlockBuilder(1);
        for (BlockReaders.IndexEntry entry : entries) {
            byte[] handle = encodeUnpaddedHandle(entry.handle());
            builder.add(entry.key(), handle);
        }
        return writeBlock(out, builder.finish());
    }

    private static byte[] encodeUnpaddedHandle(BlockHandle handle) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StorageCoding.writeVarLong(out, handle.offset());
        StorageCoding.writeVarLong(out, handle.size());
        return out.toByteArray();
    }

    private static BlockHandle writeBlock(CountingCrcOutputStream out, byte[] block) throws IOException {
        long offset = out.position();
        out.write(block);
        return new BlockHandle(offset, block.length);
    }

    private static byte[] encodeProperties(long entryCount, long dataBlockCount, Key minKey, Key maxKey,
                                           long minSequenceId, long maxSequenceId, long createdAtMillis,
                                           boolean hasTombstone) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StorageCoding.writeIntLE(out, SSTFormat.VERSION);
        StorageCoding.writeLongLE(out, entryCount);
        StorageCoding.writeIntLE(out, (int) dataBlockCount);
        writeKey(out, minKey);
        writeKey(out, maxKey);
        StorageCoding.writeLongLE(out, minSequenceId);
        StorageCoding.writeLongLE(out, maxSequenceId);
        StorageCoding.writeLongLE(out, createdAtMillis);
        StorageCoding.writeByte(out, hasTombstone ? 1 : 0);
        StorageCoding.writeByte(out, 0); // compressionType = NONE
        StorageCoding.writeByte(out, 0); // checksumType = FULL_FILE_CRC32
        return out.toByteArray();
    }

    private static void writeKey(ByteArrayOutputStream out, Key key) {
        byte[] bytes = key == null ? new byte[0] : key.bytes();
        StorageCoding.writeVarInt(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static byte[] encodeFooter(BlockHandle bloomHandle, BlockHandle indexHandle, BlockHandle propertiesHandle,
                                       int crc32) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(SSTFormat.FOOTER_SIZE);
        out.writeBytes(SSTFormat.MAGIC);
        StorageCoding.writeIntLE(out, SSTFormat.VERSION);
        out.writeBytes(StorageCoding.encodeHandle(bloomHandle));
        out.writeBytes(StorageCoding.encodeHandle(indexHandle));
        out.writeBytes(StorageCoding.encodeHandle(propertiesHandle));
        StorageCoding.writeIntLE(out, crc32);
        return out.toByteArray();
    }

    private static class CountingCrcOutputStream extends OutputStream {
        private final FileOutputStream delegate;
        private final CRC32 crc = new CRC32();
        private long position;
        private boolean footer;

        CountingCrcOutputStream(FileOutputStream delegate) {
            this.delegate = delegate;
        }

        long position() {
            return position;
        }

        long crcValue() {
            return crc.getValue();
        }

        void writeFooter(byte[] data) throws IOException {
            footer = true;
            write(data);
            footer = false;
        }

        void force() throws IOException {
            FileChannel channel = delegate.getChannel();
            channel.force(true);
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            if (!footer) {
                crc.update(b);
            }
            position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            if (!footer) {
                crc.update(b, off, len);
            }
            position += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
