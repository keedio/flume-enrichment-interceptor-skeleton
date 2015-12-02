package org.keedio.flume.interceptor.enrichment.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 2/12/15.
 */
public class EnrichmentInterceptorAbstractTest {
    // Helper methods
    protected Map<String, String> propertiesToMap(Properties props) {
        Map<String, String> m = new HashMap<String, String>();
        for (String key : props.stringPropertyNames()) {
            m.put(key, props.getProperty(key));
        }
        return m;
    }

    protected Map<String, String> mergeProps(Properties p1, Properties p2) {
        Map<String, String> m = propertiesToMap(p1);
        for (String key : p2.stringPropertyNames()) {
            m.put(key, p2.getProperty(key));
        }
        return m;
    }

    protected EnrichmentInterceptor createInterceptor(String filename, String eventType) {
        Context context = new Context();
        context.put(EnrichmentInterceptor.EVENT_TYPE, eventType);
        context.put(EnrichmentInterceptor.PROPERTIES_FILENAME, filename);

        EnrichmentInterceptor.EnrichmentBuilder builder = new EnrichmentInterceptor.EnrichmentBuilder();
        builder.configure(context);
        EnrichmentInterceptor interceptor = (EnrichmentInterceptor) builder.build();
        interceptor.initialize();
        return interceptor;
    }

    protected Event createEvent(String message) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("h1", "value1");
        return EventBuilder.withBody(message.getBytes(), headers);
    }
}
