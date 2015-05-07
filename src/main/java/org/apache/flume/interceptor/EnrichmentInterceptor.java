package org.apache.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.flume.regexp.RegexpData;

public class EnrichmentInterceptor implements Interceptor {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichmentInterceptor.class);

    static final String EVENT_TYPE = "event.type";
    static final String PROPERTIES_FILENAME = "properties.filename";

    private boolean isEnriched;
    private String filename;
    private Properties props;
    
    private RegexpData regexpData;

    public EnrichmentInterceptor(Context context) {

        try {
            String eventType = context.getString(EVENT_TYPE).toLowerCase();
            this.isEnriched = "enriched".equals(eventType);
        } catch (NullPointerException e) {
            logger.warn("Property event.type is not set. Assuming DEFAULT (not enriched).", e);
            this.isEnriched = false;
        }

        this.filename = context.getString(PROPERTIES_FILENAME);
        this.props = new Properties();
        
        regexpData = new RegexpData(context);

    }

    @Override
    public void initialize() {
        if (this.filename == null || "".equals(this.filename)) {
            logger.warn("Property file not set. Events will be enriched with empty extraData.");
        } else {
            try {
                InputStream input = new FileInputStream(this.filename);
                this.props.load(input);
                input.close();
            } catch (FileNotFoundException e) {
                logger.error("Property file not found: " + this.filename, e);
            } catch (IOException e) {
                logger.error("Error loading properties from file: " + this.filename, e);
            }
        }
    }

    @Override
    public Event intercept(Event event) {
        byte[] payload = event.getBody();
        EnrichedEventBody enrichedBody;
        try {
            enrichedBody = EnrichedEventBody.createFromEventBody(payload, isEnriched);

            Map<String, String> data = enrichedBody.getExtraData();
            for (String key : props.stringPropertyNames()) {
                data.put(key, props.getProperty(key));
            }


            Map<String, String> regexpResults = regexpData.applyRegexps(enrichedBody.getMessage());
            data.putAll(regexpResults);

            enrichedBody.setExtraData(data);
            event.setBody(enrichedBody.buildEventBody());

            logger.debug("Intercepted " + (isEnriched ? "EnrichedType" : "DefaultType") + " event:"
                            + "\n\tBody was: " + new String(payload)
                            + "\nEnriched body is:"
                            + "\n\tBody: " + new String(event.getBody())
                            + "\n\tMessage: " + enrichedBody.getMessage()
                            + "\n\tData: " + enrichedBody.getExtraData()
            );
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> out = new LinkedList<Event>();
        for (Event e : events) {
            out.add(intercept(e));
        }
        return out;
    }

    @Override
    public void close() {
        /* nothing to do, really */
    }

    Properties getProps() {
        return props;
    }

    boolean isEnriched() {
        return isEnriched;
    }

    String getFilename() {
        return filename;
    }

    public static class EnrichmentBuilder implements Interceptor.Builder {
        private Context ctx;

        @Override
        public Interceptor build() {
            return new EnrichmentInterceptor(ctx);
        }

        @Override
        public void configure(Context context) {
            this.ctx = context;
        }
    }
}
