package org.keedio.flume.interceptor.enrichment.serialization.avro.serializers;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.keedio.flume.interceptor.enrichment.serialization.Serializer;
import java.io.ByteArrayOutputStream;

/**
 * Created by PC on 06/06/2016.
 */
public class AVROSpecificSerializer<T extends SpecificRecord> implements Serializer<T> {

    private Class<T> clazz;



    public AVROSpecificSerializer(Class<T> clazz) {
        try {
            this.clazz = clazz;
            if(!SpecificRecord.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException("Class provided should implement SpecificRecord");
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in AVROSpecificSerializer constructor", e);
        }
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public void setClazz(Class<T> clazz) {
        this.clazz = clazz;
    }

    public byte[] toBytes(T object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SpecificDatumWriter<T> datumWriter = null;
        try {
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            datumWriter = new SpecificDatumWriter<T>(clazz);
            datumWriter.write(object, encoder);
            encoder.flush();
            output.close();
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in Avro specific serialization process", e);
        }
        return output.toByteArray();
    }

    public T toObject(byte[] bytes) {
        T object = null;
        SpecificDatumReader<T> reader = null;
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            reader = new SpecificDatumReader<T>(clazz);
            object =  reader.read(null, decoder);
        } catch(Exception e) {
            throw new SerializationException("An exception has thrown in Avro specific deserialization process", e);
        }

        return object;

    }

    public T toObject(byte[] bytes, Class contentClass) {
        return toObject(bytes);
    }
}
