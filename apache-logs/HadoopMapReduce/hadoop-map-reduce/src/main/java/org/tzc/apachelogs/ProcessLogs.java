package org.tzc.apachelogs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.util.Map;

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

        job.setOutputFormatClass(RedisHashOutputFormat.class);
        RedisHashOutputFormat.setRedisHosts(job, "localhost");
        RedisHashOutputFormat.setRedisHashKey(job, "first");

        boolean success = job.waitForCompletion(true);

        Jedis jedis = new Jedis("localhost");
        jedis.connect();

        /**
         * 1. Given an IP, break down how much bandwidth has been used by status code (200, 404, 500, etc).
         * Give the results in bytes as well as in percentages of the total.
         */
        String givenIp = "178.154.179.250";
        Long total = 0l;

        Map<String, String> statusIpBytes = jedis.hgetAll(givenIp);
        for (String key : statusIpBytes.keySet()) {
            total += Long.parseLong(statusIpBytes.get(key));
        }

        System.out.println("Total for IP " + givenIp + " = " + total);

        for (String statusKey : statusIpBytes.keySet()) {
            Long bytes = Long.parseLong(statusIpBytes.get(statusKey));
            Double percent = (bytes * 100.0) / total;
            System.out.println("\tStatus " + statusKey + " = " + bytes + ", " + percent + "%");
        }
    }
}
