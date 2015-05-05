package org.tzc.apachelogs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Lucian Tuca on 28/04/15.
 */
public class RedisHashRecordWriter extends RecordWriter<ALWritableComparable, LongWritable> {

    private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
    private String hashKey = null;

    public RedisHashRecordWriter(String hashKey, String hosts) {
        this.hashKey = hashKey;

        int i = 0;
        for (String host : hosts.split(",")) {
            Jedis jedis = new Jedis(host);
            jedis.connect();
            jedisMap.put(i++, jedis);
        }
    }

    @Override
    public void write(ALWritableComparable key, LongWritable value) throws IOException, InterruptedException {
        Jedis j = jedisMap.get(Math.abs(key.hashCode()) % jedisMap.size());

        j.hset(key.getApacheLogLine().getIp().toString(), key.getApacheLogLine().getStatusCode().toString(), value.toString());
    }

    public void close(TaskAttemptContext context)
            throws IOException, InterruptedException {
        for (Jedis jedis : jedisMap.values()) {
            jedis.disconnect();
        }
    }
}
