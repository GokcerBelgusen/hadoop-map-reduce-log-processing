package org.tzc.apachelogs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Lucian Tuca
 *         07/04/15
 *         HadoopMapReduce
 */
public class ALReducer extends Reducer<ALWritableComparable, ALWritableComparable, ALWritableComparable, LongWritable> {

    @Override
    public void reduce(ALWritableComparable key, Iterable<ALWritableComparable> values, Context context)
            throws IOException, InterruptedException {

        long bytesSum = 0;
        for (ALWritableComparable value : values) {
            bytesSum += value.getApacheLogLine().getTransferredBytes().get();
        }

        LongWritable outValue = new LongWritable(bytesSum);
        context.write(key, outValue);
    }
}
