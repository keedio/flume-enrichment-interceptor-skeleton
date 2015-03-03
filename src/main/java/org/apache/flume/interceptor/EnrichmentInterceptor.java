package org.apache.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class EnrichmentInterceptor implements Interceptor {

    private boolean isEnriched;
    private String filename;
    private Properties props;

    public EnrichmentInterceptor(Context context) {

        String eventType = context.getString("event.type").toLowerCase();

        this.filename = context.getString("properties.filename");
        this.isEnriched = eventType.equals("enriched");
        this.props = new Properties();

    }

    @Override
    public void initialize() {
        InputStream input;
        try {
            input = new FileInputStream(this.filename);
            this.props.load(input);
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
            enrichedBody.setExtraData(data);
            event.setBody(enrichedBody.getEventBody());
        } catch (IOException e) {
            e.printStackTrace();
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

    }

    public static class Builder implements Interceptor.Builder {
        private Context ctx;

        public Interceptor build() {
            return new EnrichmentInterceptor(ctx);
        }

        @Override
        public void configure(Context context) {
            this.ctx = context;
        }
    }
}
