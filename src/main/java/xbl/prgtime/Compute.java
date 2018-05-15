package xbl.prgtime;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import xbl.bagprg.bagprgtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Compute implements Writable {
    private int time;
    private int Count;
    private int AverageTime;


    public int getAverageTime() { return AverageTime; }
    public void setAverageTime(int AverageTime) { this.AverageTime = AverageTime;}

    public int getTime() { return time; }
    public void setTime(int time) { this.time = time;}

    public int getCount() { return Count; }
    public void setCount(int Count) { this.Count = Count;}

    public void readFields(DataInput in) throws IOException {
        time = in.readInt();
        AverageTime = in.readInt();
        Count = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(AverageTime);
        out.writeInt(time);
        out.writeInt(Count);
    }

    /*mr 输出到文件的的时候调用*/
    @Override
    public String toString() {
        return Count + "\t" + time + "\t" + AverageTime;
    }
}
