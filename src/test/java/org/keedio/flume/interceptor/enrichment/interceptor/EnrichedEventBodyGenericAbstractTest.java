package org.keedio.flume.interceptor.enrichment.interceptor;

import org.apache.avro.Schema;
import org.keedio.flume.interceptor.enrichment.serialization.SerializationBean;
import org.keedio.flume.interceptor.enrichment.serialization.SerializerFactory;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyGenericAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyMapAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.json.JSONSerializer;
import org.mozilla.universalchardet.UniversalDetector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by PC on 10/06/2016.
 */
public class EnrichedEventBodyGenericAbstractTest {


    public EnrichedEventBodyExtraData getEnrichedEventBodyExtraData() {

        EnrichedEventBodyExtraData enrichedEventBodyExtraData = new EnrichedEventBodyExtraData();
        enrichedEventBodyExtraData.setTopic("defaultTopic");
        enrichedEventBodyExtraData.setTimestamp("defaultTimestamp");
        enrichedEventBodyExtraData.setSha1Hex("defaultSha1Hex");
        enrichedEventBodyExtraData.setFilePath("defaultFilePath");
        enrichedEventBodyExtraData.setFileName("defaultFileName");
        enrichedEventBodyExtraData.setLineNumber("defaultLineNumber");
        enrichedEventBodyExtraData.setType("defaultType");

        return enrichedEventBodyExtraData;
    }

    public HashMap<String, String> getEnrichedEventBodyExtraDataHashMap() {

        HashMap<String, String> mapExtraData = new HashMap<String, String>();
        mapExtraData.put("topic","defaultTopic");
        mapExtraData.put("timestamp","defaultTimestamp");
        mapExtraData.put("sha1Hex","defaultSha1Hex");
        mapExtraData.put("filePath","defaultFilePath");
        mapExtraData.put("fileName","defaultFileName");
        mapExtraData.put("lineNumber","defaultLineNumber");
        mapExtraData.put("type","defaultType");


        return mapExtraData;
    }


    public AVROGenericSerializer getAVROGenericSerializer() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericEnrichedEventBodyExtraData.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_GENERIC_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        AVROGenericSerializer avroGenericSerializer = (AVROGenericSerializer) serializerFactory.getSerializer(serializationBean);

        return avroGenericSerializer;
    }

    public AVROGenericSerializer getAVROGenericSerializerHashMap() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericMap.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_GENERIC_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        AVROGenericSerializer avroGenericSerializer = (AVROGenericSerializer) serializerFactory.getSerializer(serializationBean);

        return avroGenericSerializer;
    }


    public AVROSpecificSerializer getAVROSpecificSerializer() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        serializationBean.setSerializerName(SerializerFactory.AVRO_SPECIFIC_SERIALIZER);
        serializationBean.setClazz(EnrichedEventBodyGenericAvroString.class);

        AVROSpecificSerializer avroSpecificSerializer = (AVROSpecificSerializer) serializerFactory.getSerializer(serializationBean);

        return avroSpecificSerializer;
    }


    public AVROSpecificSerializer getAVROSpecificSerializerHashMap() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        serializationBean.setSerializerName(SerializerFactory.AVRO_SPECIFIC_SERIALIZER);
        serializationBean.setClazz(EnrichedEventBodyMapAvroString.class);

        AVROSpecificSerializer avroSpecificSerializer = (AVROSpecificSerializer) serializerFactory.getSerializer(serializationBean);

        return avroSpecificSerializer;
    }


    public AVROReflectiveSerializer getAVROReflectiveSerializer() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericEnrichedEventBodyExtraData.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_REFLECTIVE_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        AVROReflectiveSerializer avroReflectiveSerializer = (AVROReflectiveSerializer) serializerFactory.getSerializer(serializationBean);

        return avroReflectiveSerializer;
    }


    public AVROReflectiveSerializer getAVROReflectiveHashMap() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericMap.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_REFLECTIVE_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        AVROReflectiveSerializer avroReflectiveSerializer = (AVROReflectiveSerializer) serializerFactory.getSerializer(serializationBean);

        return avroReflectiveSerializer;
    }


    public JSONSerializer getJSONSerializer() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric =
                new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>();

        serializationBean.setSerializerName(SerializerFactory.JSON_SERIALIZER);
        serializationBean.setClazz(enrichedEventBodyGeneric.getClass());

        JSONSerializer jsonSerializer = (JSONSerializer) serializerFactory.getSerializer(serializationBean);

        return jsonSerializer;
    }


    public JSONSerializer getJSONSerializerHashMap() throws IOException {

        SerializationBean serializationBean = new SerializationBean();
        SerializerFactory serializerFactory = new SerializerFactory();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric =
                new  EnrichedEventBodyGeneric<HashMap<String, String>>();

        serializationBean.setSerializerName(SerializerFactory.JSON_SERIALIZER);
        serializationBean.setClazz(enrichedEventBodyGeneric.getClass());

        JSONSerializer jsonSerializer = (JSONSerializer) serializerFactory.getSerializer(serializationBean);

        return jsonSerializer;
    }

    public EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> createEnrichedEventBodyGeneric() {

        String message = "The message áéíóúñ";
        EnrichedEventBodyExtraData enrichedEventBodyExtraData = new EnrichedEventBodyExtraData();

        enrichedEventBodyExtraData.setTopic("defaultTopic");
        enrichedEventBodyExtraData.setTimestamp("defaultTimestamp");
        enrichedEventBodyExtraData.setSha1Hex("defaultSha1Hex");
        enrichedEventBodyExtraData.setFilePath("defaultFilePath");
        enrichedEventBodyExtraData.setFileName("defaultFilename");
        enrichedEventBodyExtraData.setLineNumber("defaultLineNumber");
        enrichedEventBodyExtraData.setType("defaultType");


        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric =
                new EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(enrichedEventBodyExtraData, message, EnrichedEventBodyExtraData.class);


        return enrichedEventBodyGeneric;
    }


    public EnrichedEventBodyGeneric<HashMap<String, String>> createEnrichedEventBodyGenericHashMap() {

        String message = "The message áéíóúñ";
        HashMap<String, String> enrichedEventBodyExtraData = new HashMap<String, String>();
        Class clazz = enrichedEventBodyExtraData.getClass();

        enrichedEventBodyExtraData.put("topic","defaultTopic");
        enrichedEventBodyExtraData.put("timestamp","defaultTimestamp");
        enrichedEventBodyExtraData.put("sha1Hex","defaultSha1Hex");
        enrichedEventBodyExtraData.put("filePath","defaultFilePath");
        enrichedEventBodyExtraData.put("fileName","defaultFilename");
        enrichedEventBodyExtraData.put("lineNumber","defaultLineNumber");
        enrichedEventBodyExtraData.put("type","defaultType");


        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric =
                new EnrichedEventBodyGeneric<HashMap<String, String>>(enrichedEventBodyExtraData, message, clazz);


        return enrichedEventBodyGeneric;
    }


    public String detectCharset(byte[] output) {
        UniversalDetector detector = new UniversalDetector(null);
        detector.handleData(output, 0, output.length);
        detector.dataEnd();
        String outputCharset = detector.getDetectedCharset();
        detector.reset();
        return outputCharset;
    }
}
