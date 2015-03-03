package org.apache.flume.interceptor;

import org.apache.flume.serialization.JSONStringSerializer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnrichedEventBody {

    private Map<String, String> extraData;
    private byte[] message;

    @JsonCreator
    public EnrichedEventBody(@JsonProperty("extra_data") Map<String, String> extraData, @JsonProperty("message") byte[] message) {
        this.extraData = extraData;
        this.message = message;
    }

    public EnrichedEventBody(byte[] message) {
        this.extraData = new HashMap<String, String>();
        this.message = message;
    }

    public static EnrichedEventBody createFromEventBody(byte[] payload, boolean isEnriched) throws IOException {
        EnrichedEventBody enrichedBody;
        if (isEnriched) {
            enrichedBody = JSONStringSerializer.fromJSONString(new String(payload), EnrichedEventBody.class);
        } else {
            enrichedBody = new EnrichedEventBody(payload);
        }
        return enrichedBody;
    }

    public byte[] buildEventBody() throws IOException {
        return JSONStringSerializer.toJSONString(this).getBytes();
    }

    @Override
    public String toString() {
        String s = null;
        try {
            s = JSONStringSerializer.toJSONString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return s;
    }

    public Map<String, String> getExtraData() {
        return extraData;
    }

    public void setExtraData(Map<String, String> extraData) {
        this.extraData = extraData;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

}
