package org.qwh.pms.core.storage;

import org.qwh.pms.core.memtable.model.Key;
import org.qwh.pms.core.memtable.model.Value;
import org.qwh.pms.core.memtable.model.Entry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

final class SSTReader {
    private final SSTMeta meta;
    private final BlockHandle bloomHandle;
    private final BlockHandle indexHandle;
    private final BlockHandle propertiesHandle;
    private final BloomFilter bloomFilter;
    private final List<BlockReaders.IndexEntry> indexEntries;

    private SSTReader(SSTMeta meta, BlockHandle bloomHandle, BlockHandle indexHandle, BlockHandle propertiesHandle,
                      BloomFilter bloomFilter, List<BlockReaders.IndexEntry> indexEntries) {
        this.meta = meta;
        this.bloomHandle = bloomHandle;
        this.indexHandle = indexHandle;
        this.propertiesHandle = propertiesHandle;
        this.bloomFilter = bloomFilter;
        this.indexEntries = indexEntries;
    }

    static SSTReader open(SSTMeta meta) throws IOException {
        Footer footer = readFooter(meta.path());
        verifyFullFileCrc(meta.path(), footer.expectedCrc32);
        return openVerified(meta, footer);
    }

    static SSTReader open(Path path, SSTState state) throws IOException {
        Footer footer = readFooter(path);
        verifyFullFileCrc(path, footer.expectedCrc32);
        SSTMeta meta = readMeta(path, state, footer);
        return openVerified(meta, footer);
    }

    SSTMeta meta() {
        return meta;
    }

    private static SSTReader openVerified(SSTMeta meta, Footer footer) throws IOException {
        byte[] bloomBlock = readBlock(meta.path(), footer.bloomHandle);
        byte[] indexBlock = readBlock(meta.path(), footer.indexHandle);
        return new SSTReader(
            meta,
            footer.bloomHandle,
            footer.indexHandle,
            footer.propertiesHandle,
            BloomFilter.decode(bloomBlock),
            BlockReaders.readIndexBlock(indexBlock)
        );
    }

    Optional<Value> get(Key key) throws IOException {
        if (meta.entryCount() == 0) {
            return Optional.empty();
        }
        if (key.compareTo(meta.minKey()) < 0 || key.compareTo(meta.maxKey()) > 0) {
            return Optional.empty();
        }
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }
        BlockHandle handle = findDataBlock(key);
        if (handle == null) {
            return Optional.empty();
        }
        return BlockReaders.findInDataBlock(readBlock(meta.path(), handle), key);
    }

    SSTEntryIterator iterator() {
        return new ReaderIterator();
    }

    SSTEntryIterator iterator(Key startInclusive, Optional<Key> endExclusive) {
        if (meta.entryCount() == 0 || startInclusive.compareTo(meta.maxKey()) > 0) {
            return new ReaderIterator(indexEntries.size(), startInclusive, endExclusive);
        }
        if (endExclusive.isPresent() && endExclusive.get().compareTo(meta.minKey()) <= 0) {
            return new ReaderIterator(indexEntries.size(), startInclusive, endExclusive);
        }
        return new ReaderIterator(findDataBlockIndex(startInclusive), startInclusive, endExclusive);
    }

    private BlockHandle findDataBlock(Key key) {
        int index = findDataBlockIndex(key);
        return index >= indexEntries.size() ? null : indexEntries.get(index).handle();
    }

    private int findDataBlockIndex(Key key) {
        int left = 0;
        int right = indexEntries.size() - 1;
        int candidate = indexEntries.size();
        while (left <= right) {
            int mid = (left + right) >>> 1;
            BlockReaders.IndexEntry entry = indexEntries.get(mid);
            if (entry.key().compareTo(key) >= 0) {
                candidate = mid;
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return candidate;
    }

    static SSTMeta readMeta(Path path, SSTState state) throws IOException {
        Footer footer = readFooter(path);
        verifyFullFileCrc(path, footer.expectedCrc32);
        return readMeta(path, state, footer);
    }

    private static SSTMeta readMeta(Path path, SSTState state, Footer footer) throws IOException {
        byte[] properties = readBlock(path, footer.propertiesHandle);
        ByteArrayInputStream in = new ByteArrayInputStream(properties);
        byte[] header = StorageCoding.readExact(in, 4 + 8 + 4);
        int version = StorageCoding.readIntLE(header, 0);
        if (version != SSTFormat.VERSION) {
            throw new IllegalArgumentException("Unsupported SST version: " + version);
        }
        long entryCount = StorageCoding.readLongLE(header, 4);
        int dataBlockCount = StorageCoding.readIntLE(header, 12);
        if (dataBlockCount < 0) {
            throw new IllegalArgumentException("invalid dataBlockCount: " + dataBlockCount);
        }
        Key minKey = readKey(in, entryCount);
        Key maxKey = readKey(in, entryCount);
        byte[] tail = StorageCoding.readExact(in, 8 + 8 + 8 + 3);
        long minSequenceId = StorageCoding.readLongLE(tail, 0);
        long maxSequenceId = StorageCoding.readLongLE(tail, 8);
        long createdAtMillis = StorageCoding.readLongLE(tail, 16);
        long[] flushRange = parseFlushRange(path);
        return new SSTMeta(
            flushRange[0],
            flushRange[0],
            flushRange[1],
            path,
            Files.size(path),
            entryCount,
            minKey,
            maxKey,
            minSequenceId,
            maxSequenceId,
            createdAtMillis,
            state,
            0
        );
    }

    private static long[] parseFlushRange(Path path) {
        String name = path.getFileName().toString();
        if (!name.startsWith("sst-") || !name.endsWith(".sst")) {
            return new long[] {1, 1};
        }
        String body = name.substring(4, name.length() - 4);
        int dot = body.indexOf('.');
        String range = dot >= 0 ? body.substring(0, dot) : body;
        String[] parts = range.split("-");
        if (parts.length >= 2) {
            return new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])};
        }
        long id = Long.parseLong(range);
        return new long[] {id, id};
    }

    private static Key readKey(ByteArrayInputStream in, long entryCount) {
        int length = StorageCoding.readVarInt(in);
        byte[] bytes = new byte[length];
        int read = in.read(bytes, 0, length);
        if (read != length) {
            throw new IllegalArgumentException("truncated key in properties block");
        }
        return entryCount == 0 ? null : new Key(bytes);
    }

    private static Footer readFooter(Path path) throws IOException {
        long size = Files.size(path);
        if (size < SSTFormat.FOOTER_SIZE) {
            throw new IllegalArgumentException("SST file is too small: " + path);
        }
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            file.seek(size - SSTFormat.FOOTER_SIZE);
            byte[] footer = new byte[SSTFormat.FOOTER_SIZE];
            file.readFully(footer);
            byte[] magic = Arrays.copyOfRange(footer, 0, SSTFormat.MAGIC.length);
            if (!Arrays.equals(magic, SSTFormat.MAGIC)) {
                throw new IllegalArgumentException("bad SST magic: " + path);
            }
            int version = StorageCoding.readIntLE(footer, 8);
            if (version != SSTFormat.VERSION) {
                throw new IllegalArgumentException("Unsupported SST version: " + version);
            }
            int offset = 12;
            BlockHandle bloomHandle = StorageCoding.decodeHandle(footer, offset);
            offset += BlockHandle.MAX_ENCODED_LENGTH;
            BlockHandle indexHandle = StorageCoding.decodeHandle(footer, offset);
            offset += BlockHandle.MAX_ENCODED_LENGTH;
            BlockHandle propertiesHandle = StorageCoding.decodeHandle(footer, offset);
            offset += BlockHandle.MAX_ENCODED_LENGTH;
            int crc32 = StorageCoding.readIntLE(footer, offset);
            return new Footer(bloomHandle, indexHandle, propertiesHandle, crc32);
        }
    }

    private static byte[] readBlock(Path path, BlockHandle handle) throws IOException {
        if (handle.size() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("block too large: " + handle.size());
        }
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            file.seek(handle.offset());
            byte[] data = new byte[(int) handle.size()];
            file.readFully(data);
            return data;
        }
    }

    private static void verifyFullFileCrc(Path path, int expected) throws IOException {
        long size = Files.size(path);
        long dataLength = size - SSTFormat.FOOTER_SIZE;
        CRC32 crc = new CRC32();
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
            byte[] buffer = new byte[8192];
            long remaining = dataLength;
            while (remaining > 0) {
                int read = file.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    throw new IOException("unexpected EOF while verifying SST CRC");
                }
                crc.update(buffer, 0, read);
                remaining -= read;
            }
        }
        if ((int) crc.getValue() != expected) {
            throw new IllegalArgumentException("SST full-file CRC mismatch: " + path);
        }
    }

    private record Footer(BlockHandle bloomHandle, BlockHandle indexHandle, BlockHandle propertiesHandle,
                          int expectedCrc32) {}

    private final class ReaderIterator implements SSTEntryIterator {
        private int blockIndex;
        private Iterator<Entry> current = Collections.emptyIterator();
        private final Key startInclusive;
        private final Optional<Key> endExclusive;
        private Entry next;

        private ReaderIterator() {
            this(0, null, Optional.empty());
        }

        private ReaderIterator(int blockIndex, Key startInclusive, Optional<Key> endExclusive) {
            this.blockIndex = blockIndex;
            this.startInclusive = startInclusive;
            this.endExclusive = endExclusive;
        }

        @Override
        public boolean hasNext() {
            loadNextIfNeeded();
            return next != null;
        }

        @Override
        public Entry next() {
            loadNextIfNeeded();
            Entry result = next;
            next = null;
            return result;
        }

        private void loadNextIfNeeded() {
            if (next != null) {
                return;
            }
            while (true) {
                loadNextBlockIfNeeded();
                if (!current.hasNext()) {
                    return;
                }
                Entry candidate = current.next();
                if (startInclusive != null && candidate.key().compareTo(startInclusive) < 0) {
                    continue;
                }
                if (endExclusive.isPresent() && candidate.key().compareTo(endExclusive.get()) >= 0) {
                    current = Collections.emptyIterator();
                    blockIndex = indexEntries.size();
                    return;
                }
                next = candidate;
                return;
            }
        }

        private void loadNextBlockIfNeeded() {
            while (!current.hasNext() && blockIndex < indexEntries.size()) {
                BlockHandle handle = indexEntries.get(blockIndex++).handle();
                try {
                    current = BlockReaders.readDataBlockEntries(readBlock(meta.path(), handle)).iterator();
                } catch (IOException e) {
                    throw new RuntimeException("SST iteration failed: " + meta.path(), e);
                }
            }
        }
    }
}
