package org.keedio.flume.interceptor.enrichment.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyExtraData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyGeneric;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyMapAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyExtraDataAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyGenericAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.json.JSONSerializer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializerAbstractTest {
    static SerializerFactory serializerFactory = null;

    static {
        serializerFactory = new SerializerFactory();
    }


    public SerializerFactory getSerializerFactory() {
        return serializerFactory;
    }

    public SerializationBean createEmptySerializationBean() {
        return new SerializationBean();
    }


    public SerializationBean createSerializationBeanUnknownSerializer() {
        SerializationBean serializationBean = new SerializationBean();

        serializationBean.setSerializerName("UNKNOWN_SERIALIZER");

        return serializationBean;
    }


    public SerializationBean createSerializationBeanJSONSerializer() {
        SerializationBean serializationBean = new SerializationBean();

        serializationBean.setSerializerName(SerializerFactory.JSON_SERIALIZER);
        serializationBean.setClazz(EnrichedEventBody.class);
        return serializationBean;
    }


    public SerializationBean createSerializationBeanAVROGenericSerializer() throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyAvro.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_GENERIC_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        return serializationBean;
    }


    public SerializationBean createSerializationBeanAVROSpecificSerializer(boolean classExtendsSpecificRecord) throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        serializationBean.setSerializerName(SerializerFactory.AVRO_SPECIFIC_SERIALIZER);

        if (!classExtendsSpecificRecord) {
            //The EnrichedEventBody class doesn't implement AVRO SpecificRecord interface
            serializationBean.setClazz(EnrichedEventBody.class);
        } else {
            //The EnrichedEventBody class implements AVRO SpecificRecord interface
            //This class has been created using AvroTools
            serializationBean.setClazz(EnrichedEventBodyMapAvroString.class);
        }

        return serializationBean;
    }


    public SerializationBean createSerializationBeanAVROReflectiveSerializer() throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyAvro.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_REFLECTIVE_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        return serializationBean;
    }

    public JSONSerializer getJSONSerializer(SerializationBean serializationBean) {
        JSONSerializer JSONSerializer = (JSONSerializer) serializerFactory.getSerializer(serializationBean);
        return JSONSerializer;
    }

    public AVROGenericSerializer getAVROGenericSerializer(SerializationBean serializationBean) throws IOException{
        AVROGenericSerializer avroGenericSerializer = (AVROGenericSerializer) serializerFactory.getSerializer(serializationBean);
        return avroGenericSerializer;
    }

    public AVROSpecificSerializer getAVROSpecificSerializer(SerializationBean serializationBean) throws IOException{
        AVROSpecificSerializer avroSpecificSerializer = (AVROSpecificSerializer) serializerFactory.getSerializer(serializationBean);
        return avroSpecificSerializer;
    }

    public AVROReflectiveSerializer getAVROReflectiveSerializer(SerializationBean serializationBean) throws IOException{
        AVROReflectiveSerializer avroReflectiveSerializer = (AVROReflectiveSerializer) serializerFactory.getSerializer(serializationBean);
        return avroReflectiveSerializer;
    }

    public EnrichedEventBody createEnrichedEventBody() {
        String message = "The message áéíóúñ";
        HashMap<String, String> extraData = new HashMap<String, String>();

        extraData.put("key1", "value1 áéíóúñ");
        extraData.put("key2", "value2 áéíóúñ");

        EnrichedEventBody enrichedEventBody = new EnrichedEventBody(extraData, message);

        return enrichedEventBody;
    }



    public GenericRecord createGenericRecord(boolean isUTF8Class) throws IOException{

        String message = "The message áéíóúñ";

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyAvro.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);


        if (!isUTF8Class) { //is String class
            HashMap<String, String> extraDataString = new HashMap<String, String>();
            genericRecord.put("message", message);
            extraDataString.put("key1", "value1 áéíóúñ");
            extraDataString.put("key2", "value2 áéíóúñ");

            genericRecord.put("extraData", extraDataString);
        } else { //is org.apache.avro.util.Utf8 class
            HashMap<Utf8, Utf8> extraDataUTF8 = new HashMap<Utf8, Utf8>();
            genericRecord.put(new Utf8("message").toString(), new Utf8(message));
            extraDataUTF8.put(new Utf8("key1"), new Utf8("value1 áéíóúñ"));
            extraDataUTF8.put(new Utf8("key2"), new Utf8("value2 áéíóúñ"));

            genericRecord.put(new Utf8("extraData").toString(), extraDataUTF8);
        }

        return genericRecord;

    }

    public Schema getSchemaFromFile() throws IOException {

        Schema schema = null;

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyAvro.avsc");
        schema = new Schema.Parser().parse(avroSchemaFile);

        return schema;
    }


    public EnrichedEventBodyMapAvroString createEnrichedEventBodyMapAvroString() {

        EnrichedEventBodyMapAvroString.Builder builder = EnrichedEventBodyMapAvroString.newBuilder();

        String message = "The message áéíóúñ";

        HashMap<String, String> extraDataString = new HashMap<String, String>();
        extraDataString.put("key1", "value1 áéíóúñ");
        extraDataString.put("key2", "value2 áéíóúñ");

        builder.setMessage(message);
        builder.setExtraData(extraDataString);


        EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString = builder.build();

        return enrichedEventBodyMapAvroString;
    }

    public SpecificRecordFakeClass createSpecificRecordFakeClass() {

        SpecificRecordFakeClass specificRecordFakeClass = new SpecificRecordFakeClass();

        return specificRecordFakeClass;
    }


    class SpecificRecordFakeClass extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
        @Override
        public Schema getSchema() {
            return null;
        }

        @Override
        public Object get(int i) {
            return null;
        }

        @Override
        public void put(int i, Object o) {

        }
    }


    public Schema getSchemaGenericFromFile() throws IOException {

        Schema schema = null;

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericEnrichedEventBodyExtraData.avsc");
        schema = new Schema.Parser().parse(avroSchemaFile);

        return schema;
    }


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


    public SerializationBean createSerializationBeanAVROGenericSerializerGeneric() throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericEnrichedEventBodyExtraData.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_GENERIC_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        return serializationBean;
    }


    public SerializationBean createSerializationBeanAVROGenericSerializerGenericHashMap() throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericMap.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_GENERIC_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        return serializationBean;
    }

    public SerializationBean createSerializationBeanAVROSpecificSerializerGeneric(boolean classExtendsSpecificRecord) throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        serializationBean.setSerializerName(SerializerFactory.AVRO_SPECIFIC_SERIALIZER);

        if (!classExtendsSpecificRecord) {
            //The EnrichedEventBody class doesn't implement AVRO SpecificRecord interface
            serializationBean.setClazz(EnrichedEventBodyGeneric.class);
        } else {
            //The EnrichedEventBody class implements AVRO SpecificRecord interface
            //This class has been created using AvroTools
            serializationBean.setClazz(EnrichedEventBodyGenericAvroString.class);
        }

        return serializationBean;
    }

    public EnrichedEventBodyGenericAvroString createEnrichedEventBodyGenericAvroString() {

        EnrichedEventBodyGenericAvroString.Builder builder = EnrichedEventBodyGenericAvroString.newBuilder();

        EnrichedEventBodyExtraDataAvroString.Builder builderExtraData = EnrichedEventBodyExtraDataAvroString.newBuilder();

        String message = "The message áéíóúñ";

        builderExtraData.setTopic("defaultTopic");
        builderExtraData.setTimestamp("defaultTimestamp");
        builderExtraData.setSha1Hex("defaultSha1Hex");
        builderExtraData.setFilePath("defaultFilePath");
        builderExtraData.setFileName("defaultFilename");
        builderExtraData.setLineNumber("defaultLineNumber");
        builderExtraData.setType("defaultType");


        builder.setMessage(message);
        builder.setExtraData(builderExtraData.build());


        EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = builder.build();

        return enrichedEventBodyGenericAvroString;
    }


    public SerializationBean createSerializationBeanAVROReflectiveSerializerGeneric() throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericEnrichedEventBodyExtraData.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_REFLECTIVE_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        return serializationBean;
    }


    public SerializationBean createSerializationBeanAVROReflectiveSerializerGenericHashMap() throws IOException {
        SerializationBean serializationBean = new SerializationBean();

        File avroSchemaFile = new File("src/test/resources/avro/enrichedEventBodyGenericMap.avsc");
        Schema schema = new Schema.Parser().parse(avroSchemaFile);
        String stringSchema = schema.toString();

        serializationBean.setSerializerName(SerializerFactory.AVRO_REFLECTIVE_SERIALIZER);
        serializationBean.setSchema(stringSchema);

        return serializationBean;
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
        HashMap<String, String> mapExtraData = new HashMap<String, String>();
        Class clazz = mapExtraData.getClass();

        mapExtraData.put("key1", "value1 áéíóúñ");
        mapExtraData.put("key2", "value2 áéíóúñ");

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(mapExtraData, message, clazz);

        return enrichedEventBodyGeneric;
    }


    public SerializationBean createSerializationBeanJSONSerializerGeneric() {
        SerializationBean serializationBean = new SerializationBean();

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric =
                new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>();

        serializationBean.setSerializerName(SerializerFactory.JSON_SERIALIZER);
        serializationBean.setClazz(enrichedEventBodyGeneric.getClass());
        return serializationBean;
    }

    public SerializationBean createSerializationBeanJSONSerializerGenericWithHashMap() {
        SerializationBean serializationBean = new SerializationBean();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric =
                new  EnrichedEventBodyGeneric<HashMap<String, String>>();

        serializationBean.setSerializerName(SerializerFactory.JSON_SERIALIZER);
        serializationBean.setClazz(enrichedEventBodyGeneric.getClass());
        return serializationBean;
    }
}

