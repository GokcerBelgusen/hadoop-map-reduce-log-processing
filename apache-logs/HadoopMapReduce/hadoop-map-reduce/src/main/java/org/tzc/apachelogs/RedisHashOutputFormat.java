package org.tzc.apachelogs;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * @author Lucian Tuca
 *         07/04/15
 *         HadoopMapReduce
 */
// This output format class is templated to accept a key and value of type Text
public class RedisHashOutputFormat extends OutputFormat<ALWritableComparable, LongWritable> {

    // These static conf variables and methods are used to modify the job configuration.  This is a common pattern for MapReduce related classes to avoid the magic string problem
    public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
    public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

    public static void setRedisHosts(Job job, String hosts) {
        job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
    }

    public static void setRedisHashKey(Job job, String hashKey) {
        job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
    }

    @Override
    public RecordWriter<ALWritableComparable, LongWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
        String csvHosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
        return new RedisHashRecordWriter(hashKey, csvHosts);
    }

    public void checkOutputSpecs(JobContext job) throws IOException {
        String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
        if (hosts == null || hosts.isEmpty()) {
            throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
        }

        String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
        if (hashKey == null || hashKey.isEmpty()) {
            throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return (new NullOutputFormat<ALWritableComparable, LongWritable>()).getOutputCommitter(context);
    }

}
