package org.tzc.apachelogs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Lucian Tuca
 *         07/04/15
 *         HadoopMapReduce
 */
public class ALWritableComparable implements WritableComparable<ALWritableComparable> {

    private ApacheLogLine apacheLogLine;

    ALWritableComparable() {
    }

    ALWritableComparable(ApacheLogLine _apacheLogLine) {
        apacheLogLine = _apacheLogLine;
    }

    public ApacheLogLine getApacheLogLine() {
        return apacheLogLine;
    }

    public void setApacheLogLine(ApacheLogLine apacheLogLine) {
        this.apacheLogLine = apacheLogLine;
    }

    public void write(DataOutput dataOutput) throws IOException {
        apacheLogLine.getIp().write(dataOutput);
        apacheLogLine.getTimestamp().write(dataOutput);
        apacheLogLine.getRequestType().write(dataOutput);
        apacheLogLine.getLocation().write(dataOutput);
        apacheLogLine.getStatusCode().write(dataOutput);
        apacheLogLine.getTransferredBytes().write(dataOutput);
        apacheLogLine.getAgent().write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        Text ip = new Text();
        ip.readFields(dataInput);

        Text timestamp = new Text();
        timestamp.readFields(dataInput);

        Text requestType = new Text();
        requestType.readFields(dataInput);

        Text location = new Text();
        location.readFields(dataInput);

        IntWritable statusCode = new IntWritable();
        statusCode.readFields(dataInput);

        IntWritable transferredBytes = new IntWritable();
        transferredBytes.readFields(dataInput);

        Text agent = new Text();
        agent.readFields(dataInput);

        apacheLogLine = new ApacheLogLine(ip, timestamp, requestType, location, statusCode, transferredBytes, agent);
    }

    public int compareTo(ALWritableComparable alWritableComparable) {

        ApacheLogLine thisLogLine = apacheLogLine;
        ApacheLogLine thatLogLine = alWritableComparable.getApacheLogLine();

        if (thisLogLine.getIp().equals(thatLogLine.getIp())) {
            if (thisLogLine.getStatusCode().equals(thatLogLine.getStatusCode())) {
                return 0;
            } else {
                return thisLogLine.getStatusCode().compareTo(thatLogLine.getStatusCode());
            }
        } else {
            return thisLogLine.getIp().compareTo(thatLogLine.getIp());
        }
    }


    @Override
    public String toString() {
        return this.apacheLogLine.toString();
    }

}
