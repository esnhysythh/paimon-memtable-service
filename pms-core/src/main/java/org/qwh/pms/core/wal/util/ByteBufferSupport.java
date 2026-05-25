package org.qwh.pms.core.wal.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public final class ByteBufferSupport
{
    private static final MethodHandle INVOKE_CLEANER;

    static {
        MethodHandle invoker;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            invoker = MethodHandles.lookup()
                    .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class))
                    .bindTo(theUnsafe.get(null));
        }
        catch (Exception e) {
            try {
                Class<?> directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
                Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");

                Method cleanerMethod = directByteBufferClass.getDeclaredMethod("cleaner");
                cleanerMethod.setAccessible(true);
                MethodHandle getCleaner = MethodHandles.lookup().unreflect(cleanerMethod);

                Method cleanMethod = cleanerClass.getDeclaredMethod("clean");
                cleanerMethod.setAccessible(true);
                MethodHandle clean = MethodHandles.lookup().unreflect(cleanMethod);

                clean = MethodHandles.dropArguments(clean, 1, directByteBufferClass);
                invoker = MethodHandles.foldArguments(clean, getCleaner);
            }
            catch (Exception e1) {
                throw new AssertionError(e1);
            }
        }
        INVOKE_CLEANER = invoker;
    }

    private ByteBufferSupport()
    {
    }

    public static void unmap(MappedByteBuffer buffer)
    {
        try {
            INVOKE_CLEANER.invoke(buffer);
        }
        catch (Throwable ignored) {
            throw new AssertionError(ignored);
        }
    }
}
