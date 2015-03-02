package org.apache.flume.serialization;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JSONStringSerializer {

    private static ObjectMapper mapper = new ObjectMapper();

    public static String toJSONString(Object object) throws IOException {
        return mapper.writeValueAsString(object);
    }

    public static <T> T fromJSONString(String s, Class<T> clazz) throws IOException {
        return mapper.readValue(s, clazz);
    }

}
