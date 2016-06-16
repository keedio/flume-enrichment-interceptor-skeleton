package org.keedio.flume.interceptor.enrichment.serialization.exception;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializationException extends RuntimeException{

    public SerializationException() {
        super();
    }

    public SerializationException(String s, Throwable t) {
        super(s, t);
    }

    public SerializationException(String s) {
        super(s);
    }

    public SerializationException(Throwable t) {
        super(t);
    }
}
