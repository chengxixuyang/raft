package cn.seagull.util;

import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.nio.ByteBuffer;

/**
 * @author yupeng
 */
public class LongComparator extends AbstractComparator
{

    public LongComparator(ComparatorOptions comparatorOptions)
    {
        super(comparatorOptions);
    }

    @Override
    public String name()
    {
        return "rocksdb.java.LongComparator";
    }

    @Override
    public int compare(ByteBuffer byteBuffer1, ByteBuffer byteBuffer2)
    {
        long key1 = byteBuffer1.getLong();
        long key2 = byteBuffer2.getLong();
        long calculatedValue = key1 - key2;
        int result;
        if (calculatedValue < -2147483648L) {
            result = Integer.MIN_VALUE;
        } else if (calculatedValue > 2147483647L) {
            result = Integer.MAX_VALUE;
        } else {
            result = (int)calculatedValue;
        }

        return result;
    }
}
