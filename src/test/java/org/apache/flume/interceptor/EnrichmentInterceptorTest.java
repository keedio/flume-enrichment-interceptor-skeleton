package org.apache.flume.interceptor;

import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EnrichmentInterceptorTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichmentInterceptorTest.class);

    private Map<String, String> propertiesToMap(Properties props) {
        Map<String, String> m = new HashMap<String, String>();
        for (String key : props.stringPropertyNames()) {
            m.put(key, props.getProperty(key));
        }
        return m;
    }

    @Test
    public void testEmptyEventTypeProperty() {
        Context context = new Context();
        context.put(EnrichmentInterceptor.EVENT_TYPE, "");
        EnrichmentInterceptor interceptor = new EnrichmentInterceptor(context);
        Assert.assertFalse(interceptor.isEnriched());
    }

    @Test
    public void testMissingEventTypeProperty() {
        Context context = new Context();
        EnrichmentInterceptor interceptor = new EnrichmentInterceptor(context);
        Assert.assertFalse(interceptor.isEnriched());
    }

    @Test
    public void testEmptyFilenameProperty() {
        String emptyString = "";
        Properties emptyProps = new Properties();
        Context context = new Context();
        context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, "");
        EnrichmentInterceptor interceptor = new EnrichmentInterceptor(context);
        interceptor.initialize();
        Assert.assertTrue(interceptor.getFilename().equals(emptyString) && interceptor.getProps().equals(emptyProps));
    }

    @Test
    public void testMissingFilenameProperty() {
        Properties emptyProps = new Properties();
        Context context = new Context();
        EnrichmentInterceptor interceptor = new EnrichmentInterceptor(context);
        interceptor.initialize();
        Assert.assertTrue(interceptor.getFilename() == null && interceptor.getProps().equals(emptyProps));
    }

    @Test(enabled = true)
    public void testFullInterception() {
        String filename = "src/test/resources/interceptor.properties";

        Context context = new Context();
        context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
        context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename);

        EnrichmentInterceptor interceptor = new EnrichmentInterceptor(context);
        interceptor.initialize();

        byte[] body = "hello".getBytes();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("h1", "value1");
        Event event = EventBuilder.withBody(body, headers);
        String originalMessage = new String(event.getBody());

        interceptor.intercept(event);
        try {
            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = new String(enrichedEventBody.getMessage());

            logger.info("enriched message is: " + enrichedMessage);
            logger.info("original message is: " + originalMessage);
            Assert.assertEquals(originalMessage, enrichedMessage);

            logger.info("props are: " + interceptor.getProps());
            logger.info("extradata is: " + enrichedEventBody.getExtraData());
            Assert.assertEquals(propertiesToMap(interceptor.getProps()), enrichedEventBody.getExtraData());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
