package org.keedio.flume.interceptor.enrichment.serialization;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;

public class JSONStringSerializer {

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_EMPTY);
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    private JSONStringSerializer() {
        // nothing to do, really
    }

    public static String toJSONString(Object object) throws IOException {
        return mapper.writeValueAsString(object);
    }

    public static <T> T fromJSONString(String string, Class<T> clazz) throws IOException {
        return mapper.readValue(string, clazz);
    }

    public static byte[] toBytes(Object object) throws IOException {
        return mapper.writeValueAsBytes(object);
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) throws IOException {
        return mapper.readValue(bytes, clazz);
    }
}
