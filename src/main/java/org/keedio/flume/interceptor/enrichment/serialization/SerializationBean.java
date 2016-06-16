package org.keedio.flume.interceptor.enrichment.serialization;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializationBean {

    private String serializerName;
    private String schema;
    private Class clazz;

    public SerializationBean() {
        super();
    }

    public SerializationBean(String serializerName, String schema, Class clazz) {
        this.serializerName = serializerName;
        this.schema = schema;
        this.clazz = clazz;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSerializerName() {
        return serializerName;
    }

    public void setSerializerName(String serializerName) {
        this.serializerName = serializerName;
    }

    public Class getClazz() {
        return clazz;
    }

    public void setClazz(Class clazz) {
        this.clazz = clazz;
    }
}
