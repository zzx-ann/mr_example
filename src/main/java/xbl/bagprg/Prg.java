package xbl.bagprg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Prg implements Writable {
    private int min;


    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public void readFields(DataInput in) throws IOException {
        min = in.readInt();

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(min);

    }

    @Override
    public String toString() {
        return min +"";
    }
}
