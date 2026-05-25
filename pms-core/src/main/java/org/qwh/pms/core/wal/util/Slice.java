package org.qwh.pms.core.wal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.qwh.pms.core.wal.util.SizeOf.SIZE_OF_BYTE;
import static org.qwh.pms.core.wal.util.SizeOf.SIZE_OF_INT;
import static org.qwh.pms.core.wal.util.SizeOf.SIZE_OF_LONG;
import static org.qwh.pms.core.wal.util.SizeOf.SIZE_OF_SHORT;

public final class Slice
        implements Comparable<Slice>
{
    private final byte[] data;
    private final int offset;
    private final int length;

    private int hash;

    public Slice(int length)
    {
        data = new byte[length];
        this.offset = 0;
        this.length = length;
    }

    public Slice(byte[] data)
    {
        if (data == null) {
            throw new NullPointerException("array is null");
        }
        this.data = data;
        this.offset = 0;
        this.length = data.length;
    }

    public Slice(byte[] data, int offset, int length)
    {
        if (data == null) {
            throw new NullPointerException("array is null");
        }
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public int length()
    {
        return length;
    }

    public byte[] getRawArray()
    {
        return data;
    }

    public int getRawOffset()
    {
        return offset;
    }

    public byte getByte(int index)
    {
        checkPositionIndexes(index, index + SIZE_OF_BYTE, this.length);
        index += offset;
        return data[index];
    }

    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    public short getShort(int index)
    {
        checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
        index += offset;
        return (short) (data[index] & 0xFF | data[index + 1] << 8);
    }

    public int getInt(int index)
    {
        checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
        index += offset;
        return (data[index] & 0xff) |
                (data[index + 1] & 0xff) << 8 |
                (data[index + 2] & 0xff) << 16 |
                (data[index + 3] & 0xff) << 24;
    }

    public long getLong(int index)
    {
        checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
        index += offset;
        return ((long) data[index] & 0xff) |
                ((long) data[index + 1] & 0xff) << 8 |
                ((long) data[index + 2] & 0xff) << 16 |
                ((long) data[index + 3] & 0xff) << 24 |
                ((long) data[index + 4] & 0xff) << 32 |
                ((long) data[index + 5] & 0xff) << 40 |
                ((long) data[index + 6] & 0xff) << 48 |
                ((long) data[index + 7] & 0xff) << 56;
    }

    public void getBytes(int index, Slice dst, int dstIndex, int length)
    {
        getBytes(index, dst.data, dstIndex, length);
    }

    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
        index += offset;
        System.arraycopy(data, index, destination, destinationIndex, length);
    }

    public byte[] getBytes()
    {
        return getBytes(0, length);
    }

    public byte[] getBytes(int index, int length)
    {
        index += offset;
        if (index == 0) {
            return Arrays.copyOf(data, length);
        }
        else {
            byte[] value = new byte[length];
            System.arraycopy(data, index, value, 0, length);
            return value;
        }
    }

    public void getBytes(int index, ByteBuffer destination)
    {
        checkPositionIndex(index, this.length);
        index += offset;
        destination.put(data, index, Math.min(length, destination.remaining()));
    }

    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        out.write(data, index, length);
    }

    public int getBytes(int index, GatheringByteChannel out, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        return out.write(ByteBuffer.wrap(data, index, length));
    }

    public void setShort(int index, int value)
    {
        checkPositionIndexes(index, index + SIZE_OF_SHORT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
    }

    public void setInt(int index, int value)
    {
        checkPositionIndexes(index, index + SIZE_OF_INT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
    }

    public void setLong(int index, long value)
    {
        checkPositionIndexes(index, index + SIZE_OF_LONG, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
        data[index + 4] = (byte) (value >>> 32);
        data[index + 5] = (byte) (value >>> 40);
        data[index + 6] = (byte) (value >>> 48);
        data[index + 7] = (byte) (value >>> 56);
    }

    public void setByte(int index, int value)
    {
        checkPositionIndexes(index, index + SIZE_OF_BYTE, this.length);
        index += offset;
        data[index] = (byte) value;
    }

    public void setBytes(int index, Slice src, int srcIndex, int length)
    {
        setBytes(index, src.data, src.offset + srcIndex, length);
    }

    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        index += offset;
        System.arraycopy(source, sourceIndex, data, index, length);
    }

    public void setBytes(int index, ByteBuffer source)
    {
        checkPositionIndexes(index, index + source.remaining(), this.length);
        index += offset;
        source.get(data, index, source.remaining());
    }

    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        int readBytes = 0;
        do {
            int localReadBytes = in.read(data, index, length);
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                else {
                    break;
                }
            }
            readBytes += localReadBytes;
            index += localReadBytes;
            length -= localReadBytes;
        } while (length > 0);

        return readBytes;
    }

    public int setBytes(int index, ScatteringByteChannel in, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(buf);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                else {
                    break;
                }
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        } while (readBytes < length);

        return readBytes;
    }

    public int setBytes(int index, FileChannel in, int position, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(buf, position + readBytes);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                else {
                    break;
                }
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        } while (readBytes < length);

        return readBytes;
    }

    public Slice copySlice()
    {
        return copySlice(0, length);
    }

    public Slice copySlice(int index, int length)
    {
        checkPositionIndexes(index, index + length, this.length);

        index += offset;
        byte[] copiedArray = new byte[length];
        System.arraycopy(data, index, copiedArray, 0, length);
        return new Slice(copiedArray);
    }

    public byte[] copyBytes()
    {
        return copyBytes(0, length);
    }

    public byte[] copyBytes(int index, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        if (index == 0) {
            return Arrays.copyOf(data, length);
        }
        else {
            byte[] value = new byte[length];
            System.arraycopy(data, index, value, 0, length);
            return value;
        }
    }

    public Slice slice()
    {
        return slice(0, length);
    }

    public Slice slice(int index, int length)
    {
        if (index == 0 && length == this.length) {
            return this;
        }

        checkPositionIndexes(index, index + length, this.length);
        if (index >= 0 && length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(data, offset + index, length);
    }

    public SliceInput input()
    {
        return new SliceInput(this);
    }

    public SliceOutput output()
    {
        return new BasicSliceOutput(this);
    }

    public ByteBuffer toByteBuffer()
    {
        return toByteBuffer(0, length);
    }

    public ByteBuffer toByteBuffer(int index, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        return ByteBuffer.wrap(data, index, length).order(LITTLE_ENDIAN);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Slice slice = (Slice) o;

        if (length != slice.length) {
            return false;
        }

        if (offset == slice.offset && data == slice.data) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (data[offset + i] != slice.data[slice.offset + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        if (hash != 0) {
            return hash;
        }

        int result = length;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + data[i];
        }
        if (result == 0) {
            result = 1;
        }
        hash = result;
        return hash;
    }

    @Override
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }
        if (this.data == that.data && length == that.length && offset == that.offset) {
            return 0;
        }

        int minLength = Math.min(this.length, that.length);
        for (int i = 0; i < minLength; i++) {
            int thisByte = 0xFF & this.data[this.offset + i];
            int thatByte = 0xFF & that.data[that.offset + i];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
            }
        }
        return this.length - that.length;
    }

    public String toString(Charset charset)
    {
        return toString(0, length, charset);
    }

    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }

        return Slices.decodeString(toByteBuffer(index, length), charset);
    }

    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "length=" + length() +
                ')';
    }

    private static void checkPositionIndex(int index, int length)
    {
        if (index < 0 || index > length) {
            throw new IndexOutOfBoundsException("index: " + index + ", length: " + length);
        }
    }

    private static void checkPositionIndexes(int start, int end, int length)
    {
        if (start < 0 || end < start || end > length) {
            throw new IndexOutOfBoundsException("start: " + start + ", end: " + end + ", length: " + length);
        }
    }
}
