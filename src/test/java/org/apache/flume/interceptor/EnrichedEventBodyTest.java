package org.apache.flume.interceptor;

import org.apache.flume.serialization.JSONStringSerializer;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnrichedEventBodyTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichedEventBodyTest.class);

    @Test
    public void testCreateFromEventBody() {
        String message = "hello";
        Map<String, String> data = new HashMap<String, String>();
        data.put("k1", "v1");

        EnrichedEventBody enrichedEventBody = new EnrichedEventBody(data, message);

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBody);
            EnrichedEventBody newMessage = EnrichedEventBody.createFromEventBody(json.getBytes(), true);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMessageIntegrity() {
        String message = "hello";
        EnrichedEventBody enrichedEventBody = new EnrichedEventBody(message);
        try {
            byte[] enrichedBody = enrichedEventBody.buildEventBody();
            EnrichedEventBody retrievedBody = EnrichedEventBody.createFromEventBody(enrichedBody, true);
            Assert.assertEquals(retrievedBody.getMessage(), message);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testNullExtraData() {
        String message = "hello";
        String messageAsJsonString = "{\"message\":\"" + message + "\"}";

        try {
            // build an event from bytes with no extra_data and get its JSON string
            EnrichedEventBody fromBytes = new EnrichedEventBody(message);

            // build an event from JSON string with no extra_data
            EnrichedEventBody fromJson = JSONStringSerializer.fromJSONString(messageAsJsonString, EnrichedEventBody.class);

            logger.info("fromBytes.extraData is: " + fromBytes.getExtraData());
            logger.info("fromJSON.extraData is: " + fromJson.getExtraData());
            Assert.assertEquals(fromBytes.getExtraData(), fromJson.getExtraData());

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
