package org.apache.flume.serialization;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;

public class JSONStringSerializer {

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_EMPTY);
        mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    }

    public static String toJSONString(Object object) throws IOException {
        return mapper.writeValueAsString(object);
    }

    public static <T> T fromJSONString(String s, Class<T> clazz) throws IOException {
        return mapper.readValue(s, clazz);
    }

}
