package org.keedio.flume.interceptor.enrichment.serialization.avro.serializers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.keedio.flume.interceptor.enrichment.serialization.Serializer;
import java.io.ByteArrayOutputStream;


/**
 * Created by PC on 06/06/2016.
 */
public class AVROGenericSerializer implements Serializer<Object> {


    private Schema schema;

    public AVROGenericSerializer(Schema schema) {

        try {
            if (schema == null) {
                throw new IllegalArgumentException("schema is not provided");
            }

            this.schema = schema;

        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in AVROGenericSerializer constructor", e);
        }

    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }


    public byte[] toBytes(Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<Object> datumWriter = null;
        try {
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            datumWriter = new GenericDatumWriter<Object>(schema);
            datumWriter.write(object, encoder);
            encoder.flush();
            output.close();
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in Avro generic serialization process", e);
        }
        return output.toByteArray();
    }


    public Object toObject(byte[] bytes) {
        Object object = null;
        GenericDatumReader<Object> reader = null;
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            reader = new GenericDatumReader<Object>(schema);
            object =  reader.read(null, decoder);
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in Avro generic deserialization process", e);
        }
        return object;
    }

    public Object toObject(byte[] bytes, Class contentClass) {
        return toObject(bytes);
    }

}
