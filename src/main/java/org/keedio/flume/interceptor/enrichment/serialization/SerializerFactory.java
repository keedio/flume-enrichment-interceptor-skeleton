package org.keedio.flume.interceptor.enrichment.serialization;

import org.apache.avro.Schema;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.keedio.flume.interceptor.enrichment.serialization.json.JSONSerializer;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializerFactory {

    public static final String JSON_SERIALIZER = "json";
    public static final String AVRO_GENERIC_SERIALIZER = "avro-generic";
    public static final String AVRO_SPECIFIC_SERIALIZER = "avro-specific";
    public static final String AVRO_REFLECTIVE_SERIALIZER = "avro-reflective";


    public Serializer<?> getSerializer(SerializationBean serializationBean) {


        Serializer<?> serializer = null;

        try {

            if (serializationBean == null) {
                throw new IllegalArgumentException("Serialization bean is null");
            }

            String serializerName = serializationBean.getSerializerName();

            if ((serializerName == null) || ("".equals(serializerName))) {
                throw new IllegalArgumentException("There isn't a serializer name");
            }

            if (JSON_SERIALIZER.equals(serializerName)) {
                //Get class for serializer
                Class clazzJSONSerializer = serializationBean.getClazz();
                //Generate serializer
                serializer = new JSONSerializer<>(clazzJSONSerializer);

            } else if (AVRO_GENERIC_SERIALIZER.equals(serializerName)) {
                //Parse schema

                if (serializationBean.getSchema() == null) {
                    throw new IllegalArgumentException("There isn't a valid schema");
                }

                Schema avroSchema = new Schema.Parser().parse(serializationBean.getSchema());
                //Generate serializer
                serializer = new AVROGenericSerializer(avroSchema);


            } else if (AVRO_SPECIFIC_SERIALIZER.equals(serializerName)) {
                //Get class for serializer
                Class clazzSpecificSerializer = serializationBean.getClazz();

                if (clazzSpecificSerializer == null) {
                    throw new IllegalArgumentException("There isn't a valid class");
                }

                //Generate serializer
                serializer = new AVROSpecificSerializer(clazzSpecificSerializer);

            } else if (AVRO_REFLECTIVE_SERIALIZER.equals(serializerName)) {

                //Parse schema

                if (serializationBean.getSchema() == null) {
                    throw new IllegalArgumentException("There isn't a valid schema");
                }

                Schema avroSchema = new Schema.Parser().parse(serializationBean.getSchema());
                //Generate serializer
                serializer = new AVROReflectiveSerializer(avroSchema);


            } else {
                throw new IllegalArgumentException("Unknown serializer type: "
                        + serializerName);
            }


        } catch (SerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return serializer;

    }
}
