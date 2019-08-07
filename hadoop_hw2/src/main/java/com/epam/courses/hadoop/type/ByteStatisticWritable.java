package com.epam.courses.hadoop.type;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteStatisticWritable implements WritableComparable<ByteStatisticWritable>
{
    double averageValue;
    int totalBytes;

    public ByteStatisticWritable()
    {
        super();
    }

    public ByteStatisticWritable(double averageValue, int totalBytes)
    {
        this.averageValue = averageValue;
        this.totalBytes = totalBytes;
    }

    public double getAverageValue()
    {
        return averageValue;
    }

    public void setAverageValue(double averageValue)
    {
        this.averageValue = averageValue;
    }

    public int getTotalBytes()
    {
        return totalBytes;
    }

    public void setTotalBytes(int totalBytes)
    {
        this.totalBytes = totalBytes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ByteStatisticWritable))
            return false;

        ByteStatisticWritable that = (ByteStatisticWritable) o;

        if (Double.compare(that.getAverageValue(), getAverageValue()) != 0)
            return false;
        return getTotalBytes() == that.getTotalBytes();
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        temp = Double.doubleToLongBits(getAverageValue());
        result = (int) (temp ^ (temp >>> 32));
        result = 31 * result + getTotalBytes();
        return result;
    }

    @Override
    public String toString()
    {
        return averageValue + "," + totalBytes;
    }

    public int compareTo(ByteStatisticWritable o)
    {
        int result = this.getTotalBytes() - o.getTotalBytes();
        if (result == 0)
        {
            result = Double.compare(this.getAverageValue(), o.getAverageValue());
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(averageValue);
        dataOutput.writeInt(totalBytes);
    }

    public void readFields(DataInput dataInput) throws IOException
    {
        averageValue = dataInput.readDouble();
        totalBytes = dataInput.readInt();
    }
}
