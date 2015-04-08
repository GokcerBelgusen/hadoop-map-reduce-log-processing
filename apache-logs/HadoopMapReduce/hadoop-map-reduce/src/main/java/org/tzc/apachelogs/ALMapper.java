package org.tzc.apachelogs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ALMapper extends Mapper<LongWritable, Text, ALWritableComparable, ALWritableComparable> {

    public static final int FIELDS_NO = 7;
    public static final String LOG_PATTERN = "^(.*)\\s-\\s-\\s\\[(.*)\\]\\s\\\"(.*)\\s(/.*)\\\"\\s(\\d{3})\\s(.*)\\s\\\"-\\\"\\s(\\\".*\\\")";

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        Pattern pattern = Pattern.compile(LOG_PATTERN);
        Matcher matcher = pattern.matcher(line);

        if (matcher.matches() && FIELDS_NO == matcher.groupCount()) {

            Text ip = new Text(matcher.group(1));
            Text timestamp = new Text(matcher.group(2));
            Text requestType = new Text(matcher.group(3));
            Text location = new Text(matcher.group(4));
            IntWritable statusCode = new IntWritable(Integer.parseInt(matcher.group(5)));
            IntWritable transferredBytes = new IntWritable(Integer.parseInt(matcher.group(6)));
            Text agent = new Text(matcher.group(7));

            ApacheLogLine apacheLogLine = new ApacheLogLine(ip, timestamp, requestType, location, statusCode, transferredBytes, agent);
            context.write(new ALWritableComparable(apacheLogLine), new ALWritableComparable(apacheLogLine));
        }
    }
}


