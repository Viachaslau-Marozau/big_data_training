package com.epam.courses.hadoop.type;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteCountWritable implements WritableComparable<ByteCountWritable>
{

    int totalBytes;
    int requestCount;

    public ByteCountWritable()
    {
        super();
    }

    public ByteCountWritable(int totalBytes, int requestCount)
    {
        this.totalBytes = totalBytes;
        this.requestCount = requestCount;
    }

    public int getTotalBytes()
    {
        return totalBytes;
    }

    public void setTotalBytes(int totalBytes)
    {
        this.totalBytes = totalBytes;
    }

    public int getRequestCount()
    {
        return requestCount;
    }

    public void setRequestCount(int requestCount)
    {
        this.requestCount = requestCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ByteCountWritable))
            return false;

        ByteCountWritable that = (ByteCountWritable) o;

        if (getTotalBytes() != that.getTotalBytes())
            return false;
        return getRequestCount() == that.getRequestCount();
    }

    @Override
    public int hashCode()
    {
        int result = getTotalBytes();
        result = 31 * result + getRequestCount();
        return result;
    }

    @Override
    public String toString()
    {
        return totalBytes + "," + requestCount;
    }

    public int compareTo(ByteCountWritable o)
    {
        int result = this.getTotalBytes() - o.getTotalBytes();
        if (result == 0)
        {
            result = this.getRequestCount() - o.getRequestCount();
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(this.totalBytes);
        dataOutput.writeInt(this.requestCount);
    }

    public void readFields(DataInput dataInput) throws IOException
    {
        this.totalBytes = dataInput.readInt();
        this.requestCount = dataInput.readInt();
    }
}
