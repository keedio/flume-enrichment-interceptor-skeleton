package org.keedio.flume.interceptor.enrichment.interceptor;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by PC on 10/06/2016.
 */
public class EnrichedEventBodyExtraData {




    private String topic;
    private String timestamp;
    private String sha1Hex;
    private String filePath;
    private String fileName;
    private String lineNumber;
    private String type;


    @JsonCreator
    public EnrichedEventBodyExtraData(@JsonProperty("topic") String topic,
                                     @JsonProperty("timestamp") String timestamp,
                                     @JsonProperty("sha1Hex") String sha1Hex,
                                     @JsonProperty("filePath") String filePath,
                                     @JsonProperty("fileName") String fileName,
                                     @JsonProperty("lineNumber") String lineNumber,
                                     @JsonProperty("type") String type) {

        this.topic = topic;
        this.timestamp = timestamp;
        this.sha1Hex = sha1Hex;
        this.filePath = filePath;
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.type = type;

    }



    public EnrichedEventBodyExtraData() {}


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getSha1Hex() {
        return sha1Hex;
    }

    public void setSha1Hex(String sha1Hex) {
        this.sha1Hex = sha1Hex;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(String lineNumber) {
        this.lineNumber = lineNumber;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    boolean equals (EnrichedEventBodyExtraData enrichedEventBodyExtraData) {
        boolean isEquals = true;

        if (this == null && enrichedEventBodyExtraData != null) {
            isEquals = false;
        } else if (this != null && enrichedEventBodyExtraData == null) {
            isEquals = false;
        } else if (this == null && enrichedEventBodyExtraData == null) {
            isEquals = true;
        } else  {

            if  (this.getTopic() == null && enrichedEventBodyExtraData.getTopic() != null) {
                isEquals = false;
            } else if  (this.getTopic() != null && enrichedEventBodyExtraData.getTopic() == null) {
                isEquals = false;
            } else {
                isEquals = this.getTopic().equals(enrichedEventBodyExtraData.getTopic());
            }

            if  (this.getTimestamp() == null && enrichedEventBodyExtraData.getTimestamp() != null) {
                isEquals = false;
            } else if  (this.getTimestamp() != null && enrichedEventBodyExtraData.getTimestamp() == null) {
                isEquals = false;
            } else {
                isEquals = this.getTimestamp().equals(enrichedEventBodyExtraData.getTimestamp());
            }

            if  (this.getSha1Hex() == null && enrichedEventBodyExtraData.getSha1Hex() != null) {
                isEquals = false;
            } else if  (this.getSha1Hex() != null && enrichedEventBodyExtraData.getSha1Hex() == null) {
                isEquals = false;
            } else {
                isEquals = this.getSha1Hex().equals(enrichedEventBodyExtraData.getSha1Hex());
            }

            if  (this.getFilePath() == null && enrichedEventBodyExtraData.getFilePath() != null) {
                isEquals = false;
            } else if  (this.getFilePath() != null && enrichedEventBodyExtraData.getFilePath() == null) {
                isEquals = false;
            } else {
                isEquals = this.getFilePath().equals(enrichedEventBodyExtraData.getFilePath());
            }

            if  (this.getFileName() == null && enrichedEventBodyExtraData.getFileName() != null) {
                isEquals = false;
            } else if  (this.getFileName() != null && enrichedEventBodyExtraData.getFileName() == null) {
                isEquals = false;
            } else {
                isEquals = this.getFileName().equals(enrichedEventBodyExtraData.getFileName());
            }

            if  (this.getLineNumber() == null && enrichedEventBodyExtraData.getLineNumber() != null) {
                isEquals = false;
            } else if  (this.getLineNumber() != null && enrichedEventBodyExtraData.getLineNumber() == null) {
                isEquals = false;
            } else {
                isEquals = this.getLineNumber().equals(enrichedEventBodyExtraData.getLineNumber());
            }

            if  (this.getType() == null && enrichedEventBodyExtraData.getType() != null) {
                isEquals = false;
            } else if  (this.getType() != null && enrichedEventBodyExtraData.getType() == null) {
                isEquals = false;
            } else {
                isEquals = this.getType().equals(enrichedEventBodyExtraData.getType());
            }
        }

        return isEquals;

    }


}
