package org.tzc.apachelogs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Lucian Tuca on 06/04/15.
 */
public class ProcessLogs {

    private static Path INPUT = new Path("input");
    private static Path OUTPUT = new Path("output");

    public static void main(String[] args) throws Exception {

        Job job = new Job();
        job.setJarByClass(ProcessLogs.class);
        job.setJobName("Process Logs");

        if (args[0] != null) {
            INPUT = new Path(args[0]);
        }

        if (args[1] != null) {
            OUTPUT = new Path(args[1]);
        }

        FileInputFormat.setInputPaths(job, INPUT);
        FileSystem.getLocal(job.getConfiguration()).delete(OUTPUT, true);
        FileOutputFormat.setOutputPath(job, OUTPUT);

        job.setMapperClass(ALMapper.class);
        job.setReducerClass(ALReducer.class);

        job.setMapOutputKeyClass(ALWritableComparable.class);
        job.setMapOutputValueClass(ALWritableComparable.class);

        job.setOutputKeyClass(ALWritableComparable.class);
        job.setOutputValueClass(ALWritableComparable.class);

        boolean success = job.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}
