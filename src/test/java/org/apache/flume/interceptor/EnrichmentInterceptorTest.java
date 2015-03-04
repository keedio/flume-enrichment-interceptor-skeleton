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

    // Helper methods
    private Map<String, String> propertiesToMap(Properties props) {
        Map<String, String> m = new HashMap<String, String>();
        for (String key : props.stringPropertyNames()) {
            m.put(key, props.getProperty(key));
        }
        return m;
    }

    private Map<String, String> mergeProps(Properties p1, Properties p2) {
        Map<String, String> m = propertiesToMap(p1);
        for (String key : p2.stringPropertyNames()) {
            m.put(key, p2.getProperty(key));
        }
        return m;
    }

    private EnrichmentInterceptor createInterceptor(String filename, String eventType) {
        Context context = new Context();
        context.put(EnrichmentInterceptor.EVENT_TYPE, eventType);
        context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename);

        EnrichmentInterceptor interceptor = new EnrichmentInterceptor(context);
        interceptor.initialize();
        return interceptor;
    }

    private Event createEvent(byte[] body) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("h1", "value1");
        return EventBuilder.withBody(body, headers);
    }

    // Test suite
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

    @Test
    public void testSingleInterception() {
        try {
            Event event = createEvent("hello".getBytes());
            String originalMessage = new String(event.getBody());

            String filename = "src/test/resources/interceptor.properties";
            EnrichmentInterceptor interceptor = createInterceptor(filename, "DEFAULT");

            interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = new String(enrichedEventBody.getMessage());

            logger.info("original message is: " + originalMessage);
            logger.info("enriched message is: " + enrichedMessage);
            Assert.assertEquals(originalMessage, enrichedMessage);

            logger.info("props are: " + interceptor.getProps());
            logger.info("extradata is: " + enrichedEventBody.getExtraData());
            Assert.assertEquals(propertiesToMap(interceptor.getProps()), enrichedEventBody.getExtraData());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMultipleInterception() {

        try {
            // First interception. Inbound message has default format.
            Event event = createEvent("hello".getBytes());
            String originalMessage = new String(event.getBody());

            String filename = "src/test/resources/interceptor.properties";
            EnrichmentInterceptor interceptor = createInterceptor(filename, "DEFAULT");

            interceptor.intercept(event);

            // Second interception. Inbound message is enriched.
            String filename2 = "src/test/resources/interceptor2.properties";
            EnrichmentInterceptor interceptor2 = createInterceptor(filename2, "enriched");

            interceptor2.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = new String(enrichedEventBody.getMessage());

            Map<String, String> combinedData = mergeProps(interceptor.getProps(), interceptor2.getProps());

            logger.info("original message is: " + originalMessage);
            logger.info("enriched message is: " + enrichedMessage);
            Assert.assertEquals(originalMessage, enrichedMessage);

            logger.info("combined props are: " + combinedData);
            logger.info("extradata is: " + enrichedEventBody.getExtraData());
            Assert.assertEquals(combinedData, enrichedEventBody.getExtraData());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}