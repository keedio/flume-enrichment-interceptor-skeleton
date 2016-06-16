package org.keedio.flume.interceptor.enrichment.serialization;

/**
 * Created by PC on 06/06/2016.
 */
public interface Serializer<T> {


    public byte[] toBytes(T object);

    public T toObject(byte[] bytes);

    public T toObject(byte[] bytes, Class<T> contentClass);



}
