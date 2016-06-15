package org.keedio.flume.interceptor.enrichment.serialization.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.serialization.SerializationBean;
import org.keedio.flume.interceptor.enrichment.serialization.SerializerAbstractTest;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyMapAvroString;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializationUtilsTest extends SerializerAbstractTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SerializationUtilsTest.class);

    @Test
    public void testEnrichedEventBody2GenericRecordSchemaParse() {

        try {
            //Creation of EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Schema parse
            Schema schema = getSchemaFromFile();

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Test equality of both objects
            String messageGenericRecord = (String) genericRecord.get("message");
            HashMap<String, String> extraDataGenericRecord = (HashMap<String, String>) genericRecord.get("extraData");

            //Test GenericRecord message is equal to EnrichedEventBody message
            Assert.assertEquals(messageGenericRecord, enrichedEventBody.getMessage(), "The message is not equal");

            //Test equality of map entries
            Assert.assertEquals(enrichedEventBody.getExtraData().entrySet().size(), extraDataGenericRecord.entrySet().size(), "The extraData maps are not identical");

            Set<String> keysetGenericRecord = extraDataGenericRecord.keySet();

            for (String keyGenericRecord : keysetGenericRecord) {
                Assert.assertEquals(extraDataGenericRecord.get(keyGenericRecord), enrichedEventBody.getExtraData().get(keyGenericRecord), "The extraData maps are not identical");
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testEnrichedEventBody2GenericRecordSchemaParse");
        }
    }


    @Test
    public void testEnrichedEventBody2GenericRecordGetSchemaFromClass() {

        try{
            //Creation of EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Get schema from EnrichedEventBody class
            Schema schema = ReflectData.get().getSchema(EnrichedEventBody.class);

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Test equality of both objects
            String messageGenericRecord = (String) genericRecord.get("message");
            HashMap<String, String> extraDataGenericRecord = (HashMap<String, String>) genericRecord.get("extraData");

            //Test GenericRecord message is equal to EnrichedEventBody message
            Assert.assertEquals(messageGenericRecord, enrichedEventBody.getMessage(), "The message is not equal");

            //Test equality of map entries
            Assert.assertEquals(enrichedEventBody.getExtraData().entrySet().size(), extraDataGenericRecord.entrySet().size(), "The extraData maps are not identical");

            Set<String> keysetGenericRecord = extraDataGenericRecord.keySet();

            for (String keyGenericRecord : keysetGenericRecord) {
                Assert.assertEquals(extraDataGenericRecord.get(keyGenericRecord), enrichedEventBody.getExtraData().get(keyGenericRecord), "The extraData maps are not identical");
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testEnrichedEventBody2GenericRecordGetSchemaFromClass");
        }
    }


    @Test
    public void testGenericRecord2EnrichedEventBodyClassString() {

        try {
            //Creation of GenericRecord (String class)
            GenericRecord datum = createGenericRecord(false); //Generic Record with String class

            //Transform GenericRecord object to EnrichedEventBody object
            EnrichedEventBody enrichedEventBody = SerializationUtils.genericRecord2EnrichedEventBody(datum, false);

            //Test equality of both objects
            String messageEnrichedEventBody = enrichedEventBody.getMessage();
            String messageGenericRecord = (String) datum.get("message");
            HashMap<String, String> extraDataEnrichedEventBody = (HashMap<String, String>) enrichedEventBody.getExtraData();
            HashMap<String, String> extraDataGenericRecord = (HashMap<String, String>) datum.get("extraData");

            //Test GenericRecord message is equal to EnrichedEventBody message
            Assert.assertEquals(messageEnrichedEventBody, messageGenericRecord, "The message is not equal");

            //Test equality of map entries
            Assert.assertEquals(extraDataEnrichedEventBody.entrySet().size(), extraDataGenericRecord.entrySet().size(), "The extraData maps are not identical");

            Set<String> keysetEnrichedEventBody = extraDataEnrichedEventBody.keySet();

            for (String keyEnrichedEventBody : keysetEnrichedEventBody) {
                Assert.assertEquals(extraDataEnrichedEventBody.get(keyEnrichedEventBody), extraDataGenericRecord.get(keyEnrichedEventBody), "The extraData maps are not identical");
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testGenericRecord2EnrichedEventBodyClassString");
        }


    }


    @Test
    public void testGenericRecord2EnrichedEventBodyClassUTF8() {

        try {
            //Creation of GenericRecord (Avro Utf8 class)
            GenericRecord datum = createGenericRecord(true); //Generic Record with UTF-8 class

            //Transform GenericRecord object to EnrichedEventBody object
            EnrichedEventBody enrichedEventBody = SerializationUtils.genericRecord2EnrichedEventBody(datum, true);

            //Test equality of both objects
            String messageEnrichedEventBody = enrichedEventBody.getMessage();
            String messageGenericRecord = datum.get("message").toString();
            HashMap<String, String> extraDataEnrichedEventBody = (HashMap<String, String>) enrichedEventBody.getExtraData();
            HashMap<Utf8, Utf8> extraDataGenericRecord = (HashMap<Utf8, Utf8>) datum.get("extraData");

            //Test GenericRecord message is equal to EnrichedEventBody message
            Assert.assertEquals(messageEnrichedEventBody, messageGenericRecord, "The message is not equal");

            //Test equality of map entries
            Assert.assertEquals(extraDataEnrichedEventBody.entrySet().size(), extraDataGenericRecord.entrySet().size(), "The extraData maps are not identical");

            Set<String> keysetEnrichedEventBody = extraDataEnrichedEventBody.keySet();

            for (String keyEnrichedEventBody : keysetEnrichedEventBody) {
                String valueEnrichedEventBody = extraDataEnrichedEventBody.get(keyEnrichedEventBody);
                String valueGenericRecord = extraDataGenericRecord.get(new Utf8(keyEnrichedEventBody)).toString();

                Assert.assertEquals(valueEnrichedEventBody, valueGenericRecord, "The extraData maps are not identical");
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in genericRecord2EnrichedEventBodyClassUTF8");
        }

    }


    @Test
    public void testWriteAVROFileGenericRecord() {

        try {
            //Creation of EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Get schema from EnrichedEventBody class
            Schema schema = ReflectData.get().getSchema(EnrichedEventBody.class);

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Serialization of GenericRecord object into a AVRO file
            File fileAvro = new File("fileAvro1.avro");
            SerializationUtils.writeAVROFile(fileAvro, schema, genericRecord);


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testWriteAVROFileGenericRecord");
        }

    }


    @Test
    public void testWriteAVROFileGenericRecordCodec() {

        try {
            //Creation of EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Get schema from EnrichedEventBody class
            Schema schema = ReflectData.get().getSchema(EnrichedEventBody.class);

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Serialization of GenericRecord object into a AVRO file (with snappy codec)
            File fileAvro = new File("fileAvro2.avro");
            SerializationUtils.writeAVROFile(fileAvro, schema, genericRecord, CodecFactory.snappyCodec());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testWriteAVROFileGenericRecordCodec");
        }

    }

    @Test
    public void testWriteAVROFileGenericRecordAfterSerializationDeserialization() {

        try {

            //Obtenemos el serializador
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializer();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Creamos un EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();
            //Obtenemos el esquema a aplicar en la serializacion a partir de la clase EnrichedEventBody mediante AVRO)
            Schema schema = ReflectData.get().getSchema(EnrichedEventBody.class);

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Serialization of GenericRecord object
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(genericRecord);

            //Deserialization of byte array
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //Serialization of GenericRecord object (from deserialization) into a AVRO file
            File fileAvro = new File("fileAvro3.avro");
            SerializationUtils.writeAVROFile(fileAvro, schema, genericRecordDeserialized);


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testWriteAVROFileGenericRecordAfterSerializationDeserialization");
        }

    }


    @Test
    public void testWriteAVROFileGenericRecordCodecAfterSerializationDeserialization() {

        try {

            //Obtenemos el serializador
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializer();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Creation of EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Get schema from EnrichedEventBody class
            Schema schema = ReflectData.get().getSchema(EnrichedEventBody.class);

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Serialization of GenericRecord object
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(genericRecord);

            //Deserialization of byte array
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //Serialization of GenericRecord object (from deserialization) into a AVRO file (with snappy codec)
            File fileAvro = new File("fileAvro4.avro");
            SerializationUtils.writeAVROFile(fileAvro, schema, genericRecordDeserialized, CodecFactory.snappyCodec());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testWriteAVROFileGenericRecordCodecAfterSerializationDeserialization");
        }

    }


    @Test
    public void testWriteAVROFileEnrichedEventBodyAvroString() {

        try {
            //Creation of EnrichedEventBodyMapAvroString
            EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString = createEnrichedEventBodyMapAvroString();

            //Serialization of GenericRecord object into a AVRO file
            File fileAvro = new File("fileAvro5.avro");
            SerializationUtils.writeAVROFile(fileAvro, enrichedEventBodyMapAvroString);


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testWriteAVROFileEnrichedEventBodyAvroString");
        }

    }


}
