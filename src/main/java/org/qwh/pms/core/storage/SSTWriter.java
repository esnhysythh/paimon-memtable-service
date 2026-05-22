package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.ImmutableMemTable;
import org.qwh.pms.core.memtable.model.Entry;
import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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

    SSTMeta write(long fileId, ImmutableMemTable memTable) throws IOException {
        Files.createDirectories(dir);
        Path tmp = dir.resolve(String.format("sst-%06d.new.sst.tmp", fileId));
        Path target = dir.resolve(String.format("sst-%06d.new.sst", fileId));
        Files.deleteIfExists(tmp);

        List<BlockReaders.IndexEntry> indexEntries = new ArrayList<>();
        BloomFilter bloom = BloomFilter.create(Math.max(1, memTable.estimatedEntryCount()), SSTFormat.DEFAULT_BLOOM_FPP);
        DataBlockBuilder blockBuilder = new DataBlockBuilder(restartInterval);
        Key minKey = null;
        Key maxKey = null;
        boolean hasTombstone = false;
        long entryCount = 0;
        long dataBlockCount = 0;

        try (CountingCrcOutputStream out = new CountingCrcOutputStream(
            Files.newOutputStream(tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            Iterator<Entry> iterator = memTable.iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                if (minKey == null) {
                    minKey = entry.key();
                }
                maxKey = entry.key();
                hasTombstone |= entry.value().isTombstone();
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
            byte[] properties = encodeProperties(
                entryCount,
                dataBlockCount,
                minKey,
                maxKey,
                memTable.minSequenceId(),
                memTable.maxSequenceId(),
                System.currentTimeMillis(),
                hasTombstone
            );
            BlockHandle propertiesHandle = writeBlock(out, properties);
            long fileSize = out.position() + SSTFormat.FOOTER_SIZE;
            byte[] footer = encodeFooter(bloomHandle, indexHandle, propertiesHandle, (int) out.crcValue());
            out.writeFooter(footer);
            out.flush();

            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return new SSTMeta(
                fileId,
                target,
                fileSize,
                entryCount,
                minKey,
                maxKey,
                memTable.minSequenceId(),
                memTable.maxSequenceId(),
                System.currentTimeMillis(),
                SSTState.NEW,
                0
            );
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
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
        private final OutputStream delegate;
        private final CRC32 crc = new CRC32();
        private long position;
        private boolean footer;

        CountingCrcOutputStream(OutputStream delegate) {
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
