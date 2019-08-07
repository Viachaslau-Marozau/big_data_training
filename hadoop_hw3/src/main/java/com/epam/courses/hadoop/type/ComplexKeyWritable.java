package com.epam.courses.hadoop.type;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComplexKeyWritable implements WritableComparable<ComplexKeyWritable>
{
    private int sityCode;
    private String operationSystem;

    public ComplexKeyWritable()
    {
    }

    public ComplexKeyWritable(int sityCode, String operationSystem)
    {
        this.sityCode = sityCode;
        this.operationSystem = operationSystem;
    }

    public int getSityCode()
    {
        return sityCode;
    }

    public void setSityCode(int sityCode)
    {
        this.sityCode = sityCode;
    }

    public String getOperationSystem()
    {
        return operationSystem;
    }

    public void setOperationSystem(String operationSystem)
    {
        this.operationSystem = operationSystem;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ComplexKeyWritable))
            return false;

        ComplexKeyWritable that = (ComplexKeyWritable) o;

        if (getSityCode() != that.getSityCode())
            return false;
        return getOperationSystem().equals(that.getOperationSystem());
    }

    @Override
    public int hashCode()
    {
        int result = getSityCode();
        result = 31 * result + getOperationSystem().hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return sityCode + "," + operationSystem;
    }

    @Override
    public int compareTo(ComplexKeyWritable o)
    {
        int result = sityCode - o.getSityCode();
        if (result != 0)
        {
            return result;
        }
        return operationSystem.compareTo(o.getOperationSystem());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(sityCode);
        dataOutput.writeUTF(operationSystem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        sityCode = dataInput.readInt();
        operationSystem = dataInput.readUTF();
    }
}
