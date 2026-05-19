package org.qwh.pms.core.wal.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

public abstract class SliceOutput
        extends OutputStream
        implements DataOutput
{
    public abstract void reset();

    public abstract int size();

    public abstract int writableBytes();

    public abstract boolean isWritable();

    @Override
    public final void writeBoolean(boolean value)
    {
        writeByte(value ? 1 : 0);
    }

    @Override
    public final void write(int value)
    {
        writeByte(value);
    }

    @Override
    public abstract void writeByte(int value);

    @Override
    public abstract void writeShort(int value);

    @Override
    public abstract void writeInt(int value);

    @Override
    public abstract void writeLong(long value);

    public abstract void writeBytes(Slice source);

    public abstract void writeBytes(SliceInput source, int length);

    public abstract void writeBytes(Slice source, int sourceIndex, int length);

    @Override
    public final void write(byte[] source)
            throws IOException
    {
        writeBytes(source);
    }

    public abstract void writeBytes(byte[] source);

    @Override
    public final void write(byte[] source, int sourceIndex, int length)
    {
        writeBytes(source, sourceIndex, length);
    }

    public abstract void writeBytes(byte[] source, int sourceIndex, int length);

    public abstract void writeBytes(ByteBuffer source);

    public abstract int writeBytes(InputStream in, int length)
            throws IOException;

    public abstract int writeBytes(ScatteringByteChannel in, int length)
            throws IOException;

    public abstract int writeBytes(FileChannel in, int position, int length)
            throws IOException;

    public abstract void writeZero(int length);

    public abstract Slice slice();

    public abstract ByteBuffer toByteBuffer();

    public abstract String toString(Charset charset);

    @Override
    public void writeChar(int value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeFloat(float v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChars(String s)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUTF(String s)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(String s)
    {
        throw new UnsupportedOperationException();
    }
}
