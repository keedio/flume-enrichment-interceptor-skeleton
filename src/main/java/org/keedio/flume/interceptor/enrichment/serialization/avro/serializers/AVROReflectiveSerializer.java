package org.keedio.flume.interceptor.enrichment.serialization.avro.serializers;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.keedio.flume.interceptor.enrichment.serialization.Serializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;

import java.io.ByteArrayOutputStream;

/**
 * Created by PC on 08/06/2016.
 */
public class AVROReflectiveSerializer<T> implements Serializer<T> {



    private Schema schema;


    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public AVROReflectiveSerializer(Schema schema) {

        try {
            if (schema == null) {
                throw new IllegalArgumentException("schema is not provided");
            }

            this.schema = schema;

        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in AVROReflectiveSerializer constructor", e);
        }
    }




    public byte[] toBytes(T object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ReflectDatumWriter<T> datumWriter = null;
        try {
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            datumWriter = new ReflectDatumWriter<T>(schema);
            datumWriter.write(object, encoder);
            encoder.flush();
            output.close();
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in Avro reflect serialization process", e);
        }
        return output.toByteArray();
    }



    public T toObject(byte[] bytes) {
        T object = null;
        ReflectDatumReader<T> reader = null;
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            reader = new ReflectDatumReader<T>(schema);
            object =  reader.read(null, decoder);
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in Avro reflect deserialization process", e);
        }

        return object;

    }

    public T toObject(byte[] bytes, Class<T> contentClass) {
        return toObject(bytes);
    }

}
