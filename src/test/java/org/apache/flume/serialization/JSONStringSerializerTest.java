package org.apache.flume.serialization;

import org.apache.flume.interceptor.EnrichedEventBody;
import org.apache.flume.interceptor.EnrichmentInterceptor;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class JSONStringSerializerTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichmentInterceptor.class);

    @Test(enabled = false)
    public void testNonEmptySerialization() {
        byte[] message = "hello".getBytes();
        String messageAsJsonString = "{\"message\":\"aGVsbG8=\"}";

        try {
            // build an event from bytes with no extra_data and get its JSON string
            EnrichedEventBody fromBytes = new EnrichedEventBody(message);
            String jsonFromBytes = JSONStringSerializer.toJSONString(fromBytes);

            logger.info("json fromBytes is: " + jsonFromBytes);
            logger.info("Original message is: " + messageAsJsonString);
            Assert.assertEquals(jsonFromBytes, messageAsJsonString);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
