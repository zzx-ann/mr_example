package xbl.fields;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable {
    private int min;
    private int max;
    private int count;

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }


    public void readFields(DataInput in) throws IOException {
        min = in.readInt();
        max = in.readInt();
        count = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
        out.writeInt(count);
    }

    @Override
    public String toString() {
        return min + "\t" + max + "\t" + count;
    }
}