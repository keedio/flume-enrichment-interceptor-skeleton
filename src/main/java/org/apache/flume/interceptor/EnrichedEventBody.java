package org.apache.flume.interceptor;

import org.apache.flume.serialization.JSONStringSerializer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnrichedEventBody {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichedEventBody.class);

    private Map<String, String> extraData;
    private String message;

    @JsonCreator
    public EnrichedEventBody(@JsonProperty("extraData") Map<String, String> extraData,
                             @JsonProperty("message") String message) {
        if (extraData == null) {
            this.extraData = new HashMap<String, String>();
        } else {
            this.extraData = extraData;
        }
        this.message = message;
    }

    public EnrichedEventBody(String message) {
        this.extraData = new HashMap<String, String>();
        this.message = message;
    }

    public static EnrichedEventBody createFromEventBody(byte[] payload, boolean isEnriched) throws IOException {
        EnrichedEventBody enrichedBody;
        String message = new String(payload);
        if (isEnriched) {
            enrichedBody = JSONStringSerializer.fromJSONString(message, EnrichedEventBody.class);
        } else {
            enrichedBody = new EnrichedEventBody(message);
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
            logger.error(e.getMessage(), e);
        }
        return s;
    }

    public Map<String, String> getExtraData() {
        return extraData;
    }

    public void setExtraData(Map<String, String> extraData) {
        this.extraData = extraData;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
