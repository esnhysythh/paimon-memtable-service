package org.qwh.pms.core.wal.util;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.Charset;

public final class SliceInput
        extends InputStream
        implements DataInput
{
    private final Slice slice;
    private int position;

    public SliceInput(Slice slice)
    {
        this.slice = slice;
    }

    public int position()
    {
        return position;
    }

    public void setPosition(int position)
    {
        if (position < 0 || position > slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        this.position = position;
    }

    public boolean isReadable()
    {
        return available() > 0;
    }

    @Override
    public int available()
    {
        return slice.length() - position;
    }

    @Override
    public boolean readBoolean()
            throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        return readByte();
    }

    @Override
    public byte readByte()
    {
        if (position == slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        return slice.getByte(position++);
    }

    @Override
    public int readUnsignedByte()
    {
        return (short) (readByte() & 0xFF);
    }

    @Override
    public short readShort()
    {
        short v = slice.getShort(position);
        position += 2;
        return v;
    }

    @Override
    public int readUnsignedShort()
            throws IOException
    {
        return readShort() & 0xff;
    }

    @Override
    public int readInt()
    {
        int v = slice.getInt(position);
        position += 4;
        return v;
    }

    public long readUnsignedInt()
    {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public long readLong()
    {
        long v = slice.getLong(position);
        position += 8;
        return v;
    }

    public byte[] readByteArray(int length)
    {
        byte[] value = slice.copyBytes(position, length);
        position += length;
        return value;
    }

    public Slice readBytes(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        Slice value = slice.slice(position, length);
        position += length;
        return value;
    }

    public Slice readSlice(int length)
    {
        Slice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }

    @Override
    public void readFully(byte[] destination)
    {
        readBytes(destination);
    }

    public void readBytes(byte[] destination)
    {
        readBytes(destination, 0, destination.length);
    }

    @Override
    public void readFully(byte[] destination, int offset, int length)
    {
        readBytes(destination, offset, length);
    }

    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    public void readBytes(Slice destination)
    {
        readBytes(destination, destination.length());
    }

    public void readBytes(Slice destination, int length)
    {
        if (length > destination.length()) {
            throw new IndexOutOfBoundsException();
        }
        readBytes(destination, destination.length(), length);
    }

    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    public void readBytes(ByteBuffer destination)
    {
        int length = destination.remaining();
        slice.getBytes(position, destination);
        position += length;
    }

    public int readBytes(GatheringByteChannel out, int length)
            throws IOException
    {
        int readBytes = slice.getBytes(position, out, length);
        position += readBytes;
        return readBytes;
    }

    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        slice.getBytes(position, out, length);
        position += length;
    }

    public int skipBytes(int length)
    {
        length = Math.min(length, available());
        position += length;
        return length;
    }

    public Slice slice()
    {
        return slice.slice(position, available());
    }

    public ByteBuffer toByteBuffer()
    {
        return slice.toByteBuffer(position, available());
    }

    public String toString(Charset charset)
    {
        return slice.toString(position, available(), charset);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "ridx=" + position + ", " +
                "cap=" + slice.length() +
                ')';
    }

    @Override
    public char readChar()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readLine()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF()
    {
        throw new UnsupportedOperationException();
    }
}
