package org.keedio.flume.interceptor.enrichment.interceptor;

import com.google.common.hash.Hashing;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichmentInterceptor;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.mozilla.universalchardet.UniversalDetector;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

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
        return createInterceptor(context);
    }

    private EnrichmentInterceptor createInterceptor(Context context) {

        EnrichmentInterceptor.EnrichmentBuilder builder = new EnrichmentInterceptor.EnrichmentBuilder();
        builder.configure(context);
        EnrichmentInterceptor interceptor = (EnrichmentInterceptor) builder.build();
        interceptor.initialize();
        return interceptor;
    }

    private Event createEvent(String message) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("h1", "value1");
        return EventBuilder.withBody(message.getBytes(), headers);
    }

    private Event createEvent(byte[] message) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("h1", "value1");
        return EventBuilder.withBody(message, headers);
    }

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
            Assert.assertNull(enrichedEventBody.getExtraData().get(EnrichmentInterceptor.EXTRADATA_HASH_KEY));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSingleInterceptionWithHash() throws NoSuchAlgorithmException {
        try {
            Event event = createEvent("hello");
            String originalMessage = new String(event.getBody());

            String filename = "src/test/resources/interceptor.properties";
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename);
            context.put(EnrichmentInterceptor.ENABLE_MESSAGE_HASHING, Boolean.TRUE.toString());

            EnrichmentInterceptor interceptor = createInterceptor(context);

            interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = enrichedEventBody.getMessage();

            logger.info("original message is: " + originalMessage);
            logger.info("enriched message is: " + enrichedMessage);
            Assert.assertEquals(originalMessage, enrichedMessage);

            Assert.assertEquals(Hashing.sha256().hashString(originalMessage).toString(),
                    enrichedEventBody.getExtraData().get(EnrichmentInterceptor.EXTRADATA_HASH_KEY));
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
    public void testMultipleInterceptionWithHash() {

        try {
            // First interception. Inbound message has default format.
            Event event = createEvent("hello");
            String originalMessage = new String(event.getBody());

            String filename = "src/test/resources/interceptor.properties";
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename);
            context.put(EnrichmentInterceptor.ENABLE_MESSAGE_HASHING, Boolean.TRUE.toString());

            EnrichmentInterceptor interceptor = createInterceptor(context);

            interceptor.intercept(event);

            // Second interception. Inbound message is enriched.
            String filename2 = "src/test/resources/interceptor2.properties";
            context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "enriched");
            context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename2);
            context.put(EnrichmentInterceptor.ENABLE_MESSAGE_HASHING, Boolean.TRUE.toString());
            EnrichmentInterceptor interceptor2 = createInterceptor(context);

            interceptor2.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
            String enrichedMessage = enrichedEventBody.getMessage();


            logger.info("original message is: " + originalMessage);
            logger.info("enriched message is: " + enrichedMessage);
            Assert.assertEquals(originalMessage, enrichedMessage);

            Assert.assertEquals(Hashing.sha256().hashString(originalMessage).toString(),
                    enrichedEventBody.getExtraData().get(EnrichmentInterceptor.EXTRADATA_HASH_KEY));
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

    @Test
    public void testInterceptionWithNonStandardCharset() throws IOException {

        Path path = Paths.get("src/test/resources/notUTFString.txt");
        byte[] payload = Files.readAllBytes(path);

        Event event = createEvent(payload);

        String filename = "src/test/resources/interceptor.properties";
        Context context = new Context();
        context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
        context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename);
        context.put(EnrichmentInterceptor.ENABLE_MESSAGE_HASHING, Boolean.TRUE.toString());

        EnrichmentInterceptor interceptor = createInterceptor(context);

        interceptor.intercept(event);

        EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(event.getBody(), true);
        String enrichedMessage = enrichedEventBody.getMessage();

        UniversalDetector detector = new UniversalDetector(null);
        detector.handleData(payload, 0, payload.length);
        detector.dataEnd();
        String outputCharset = detector.getDetectedCharset();
        detector.reset();

        String originalMessage = new String(payload, outputCharset);

        logger.info("original message is: " + originalMessage);
        logger.info("enriched message is: " + enrichedMessage);
        Assert.assertEquals(originalMessage, enrichedMessage);

        Assert.assertEquals(Hashing.sha256().hashString(originalMessage).toString(),
                enrichedEventBody.getExtraData().get(EnrichmentInterceptor.EXTRADATA_HASH_KEY));
    }
}
