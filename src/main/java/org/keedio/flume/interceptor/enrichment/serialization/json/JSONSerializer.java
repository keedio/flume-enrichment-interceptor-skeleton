package org.keedio.flume.interceptor.enrichment.serialization.json;

import org.codehaus.jackson.type.JavaType;
import org.keedio.flume.interceptor.enrichment.serialization.JSONStringSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.Serializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;


/**
 * Created by PC on 06/06/2016.
 */
public class JSONSerializer<T> implements Serializer<T> {

    private Class<T> clazz;

    public JSONSerializer(Class<T> clazz) {
        try {
            if (clazz == null) {
                throw new IllegalArgumentException("The class parameter is null");
            }
            this.clazz = clazz;


        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in JSONSerializer constructor", e);
        }
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public void setClazz(Class<T> clazz) {
        this.clazz = clazz;
    }


    public byte[] toBytes(T object) {
        try {
            return JSONStringSerializer.toBytes(object);
        } catch (Exception e) {
            throw new SerializationException("\"An exception has thrown in JSON serialization process", e);
        }

    }

    public T toObject(byte[] bytes) {
        try {
            return JSONStringSerializer.fromBytes(bytes, clazz);
        } catch (Exception e) {
            throw new SerializationException("An exception has thrown in JSON deserialization process", e);
        }
    }


    public T toObject(byte[] bytes, Class<T> contentClazz) {
        try {
            JavaType javaType = JSONStringSerializer.getJavaType(clazz, contentClazz);
            return JSONStringSerializer.fromBytes(bytes, javaType);
        } catch (Exception e) {
            throw new SerializationException("An exception has thrown in JSON deserialization process", e);
        }
    }


    public String toJSONString(Object object) {
        try {
            return JSONStringSerializer.toJSONString(object);
        } catch (Exception e) {
            throw new SerializationException("An exception has thrown in JSON serialization process", e);
        }
    }


    public T fromJSONString(String string) {
        try {
            return JSONStringSerializer.fromJSONString(string, clazz);
        } catch (Exception e) {
            throw new SerializationException("An exception has thrown in JSON deserialization process", e);
        }
    }


    public T fromJSONString(String string,  Class<T> contentClazz) {
        try {
            JavaType javaType = JSONStringSerializer.getJavaType(clazz, contentClazz);
            return JSONStringSerializer.fromJSONString(string, javaType);
        } catch (Exception e) {
            throw new SerializationException("An exception has thrown in JSON deserialization process", e);
        }
    }

}
