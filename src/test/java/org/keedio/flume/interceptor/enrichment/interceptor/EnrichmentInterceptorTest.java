package org.keedio.flume.interceptor.enrichment.interceptor;

import org.keedio.flume.interceptor.enrichment.interceptor.EnrichmentInterceptor;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

public class EnrichmentInterceptorTest extends EnrichmentInterceptorAbstractTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichmentInterceptorTest.class);

    // Test suite
    @Test
    public void testEmptyEventTypeProperty() {
        EnrichmentInterceptor interceptor = createInterceptor(null, "");
        Assert.assertFalse(interceptor.isEnriched());
    }

    @Test
    public void testMissingEventTypeProperty() {
        EnrichmentInterceptor interceptor = createInterceptor(null, null);
        Assert.assertFalse(interceptor.isEnriched());
    }

    @Test
    public void testEmptyFilenameProperty() {
        String emptyString = "";
        Properties emptyProps = new Properties();
        EnrichmentInterceptor interceptor = createInterceptor("", null);
        Assert.assertTrue(interceptor.getFilename().equals(emptyString) && interceptor.getProps().equals(emptyProps));
    }

    @Test
    public void testMissingFilenameProperty() {
        Properties emptyProps = new Properties();
        EnrichmentInterceptor interceptor = createInterceptor(null, null);
        Assert.assertTrue(interceptor.getFilename() == null && interceptor.getProps().equals(emptyProps));
    }

    @Test
    public void testSingleInterception() {
        try {
            Event event = createEvent("hello");
            String originalMessage = new String(event.getBody());

            String filename = "src/test/resources/interceptor.properties";
            EnrichmentInterceptor interceptor = createInterceptor(filename, "DEFAULT");

            interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = enrichedEventBody.getMessage();

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
            Event event = createEvent("hello");
            String originalMessage = new String(event.getBody());

            String filename = "src/test/resources/interceptor.properties";
            EnrichmentInterceptor interceptor = createInterceptor(filename, "DEFAULT");

            interceptor.intercept(event);

            // Second interception. Inbound message is enriched.
            String filename2 = "src/test/resources/interceptor2.properties";
            EnrichmentInterceptor interceptor2 = createInterceptor(filename2, "enriched");

            interceptor2.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = enrichedEventBody.getMessage();

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

    @Test
    public void testListInterception() {
        Event e1 = createEvent("hello1");
        Event e2 = createEvent("hello2");
        List<Event> eventList = new LinkedList<Event>();
        eventList.add(e1);
        eventList.add(e2);
        int size = eventList.size();

        String filename = "src/test/resources/interceptor.properties";
        EnrichmentInterceptor interceptor = createInterceptor(filename, "DEFAULT");

        List<Event> interceptedList = interceptor.intercept(eventList);

        Assert.assertEquals(size, interceptedList.size());
    }
}
