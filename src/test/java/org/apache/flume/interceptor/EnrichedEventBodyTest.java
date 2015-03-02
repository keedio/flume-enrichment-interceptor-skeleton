package org.apache.flume.interceptor;

import org.apache.flume.serialization.JSONStringSerializer;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnrichedEventBodyTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichmentInterceptor.class);

    @Test
    public void testCreateFromByteArray() {
        // Build a enriched event payload
        byte[] bytes = "hello".getBytes();
        Map<String, String> data = new HashMap<String, String>();
        data.put("k1", "v1");

        EnrichedEventBody enrichedEventBody = new EnrichedEventBody(data, bytes);

        String json = null;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBody);
            EnrichedEventBody newMessage = EnrichedEventBody.createFromByteArray(json.getBytes(), true);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }
}
