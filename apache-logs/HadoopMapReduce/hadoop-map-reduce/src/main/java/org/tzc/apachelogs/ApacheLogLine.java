package org.tzc.apachelogs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by Lucian Tuca on 07/04/15.
 */
public class ApacheLogLine {
    private Text ip;
    private Text timestamp;
    private Text requestType;
    private Text location;
    private IntWritable statusCode;
    private IntWritable transferredBytes;
    private Text agent;

    ApacheLogLine(Text _ip, Text _timestamp, Text _requestType, Text _location, IntWritable _statusCode, IntWritable _transfferedBytes,
                  Text _agent) {
        setIp(_ip);
        setTimestamp(_timestamp);
        setRequestType(_requestType);
        setLocation(_location);
        setStatusCode(_statusCode);
        setTransferredBytes(_transfferedBytes);
        setAgent(_agent);
    }

    public Text getIp() {
        return ip;
    }

    public void setIp(Text ip) {
        this.ip = ip;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Text timestamp) {
        this.timestamp = timestamp;
    }

    public Text getRequestType() {
        return requestType;
    }

    public void setRequestType(Text requestType) {
        this.requestType = requestType;
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public IntWritable getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(IntWritable statusCode) {
        this.statusCode = statusCode;
    }

    public IntWritable getTransferredBytes() {
        return transferredBytes;
    }

    public void setTransferredBytes(IntWritable transferredBytes) {
        this.transferredBytes = transferredBytes;
    }

    public Text getAgent() {
        return agent;
    }

    public void setAgent(Text agent) {
        this.agent = agent;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Ip: ").append(ip.toString());
        stringBuilder.append("Timestamp: ").append(timestamp.toString());
        stringBuilder.append("Request type: ").append(requestType.toString());
        stringBuilder.append("Location: ").append(location.toString());
        stringBuilder.append("Status code: ").append(statusCode.toString());
        stringBuilder.append("Transferred bytes: ").append(transferredBytes.toString());
        stringBuilder.append("Agent: ").append(agent.toString());
        return stringBuilder.toString();
    }
}
