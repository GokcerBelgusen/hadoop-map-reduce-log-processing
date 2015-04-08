package org.tzc.apachelogs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ALReducer extends Reducer<ALWritableComparable, ALWritableComparable, Text, LongWritable> {

    @Override
    public void reduce(ALWritableComparable key, Iterable<ALWritableComparable> values, Context context)
            throws IOException, InterruptedException {

        long bytesSum = 0;
        for (ALWritableComparable value : values) {
            bytesSum += value.getApacheLogLine().getTransferredBytes().get();
        }

        Text outKey = new Text(key.getApacheLogLine().getIp() + " " + key.getApacheLogLine().getStatusCode());
        LongWritable outValue = new LongWritable(bytesSum);

        context.write(outKey, outValue);
    }
}
