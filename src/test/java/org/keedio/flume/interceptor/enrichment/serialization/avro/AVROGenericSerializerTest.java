package org.keedio.flume.interceptor.enrichment.serialization.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyExtraData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyGeneric;
import org.keedio.flume.interceptor.enrichment.serialization.SerializationBean;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.keedio.flume.interceptor.enrichment.serialization.SerializerAbstractTest;
import org.keedio.flume.interceptor.enrichment.serialization.utils.SerializationUtils;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by PC on 06/06/2016.
 */
public class AVROGenericSerializerTest extends SerializerAbstractTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AVROGenericSerializerTest.class);


    @Test
    public void testAVROGenericSerializerWithoutSchemaFromSerializerFactory() {

        try {

            //Get serializer from factory
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializer();
            serializationBean.setSchema(null);
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerWithoutSchemaFromSerializerFactory");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerWithoutSchemaFromSerializerFactory");
        }
    }


    @Test
    public void testAVROGenericSerializerWithoutSchema() {

        try {

            //Get serializer directly (not from Serializer factory)
            AVROGenericSerializer avroGenericSerializer = new AVROGenericSerializer(null);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerWithoutSchema");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerWithoutSchema");
        }
    }


    @Test
    public void testAVROGenericSerializerSerializationError() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializer();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a EnrichedEventBody object (this class doesn't implement required interface)
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialize EnrichedEventBody object (EnrichedEventBody class doesn't implement necessary interface)
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(enrichedEventBody);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationError");
        }

    }


    @Test
    public void testAVROGenericSerializerDeserializationError() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializer();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a null byteArray
            byte[] byteArrayAVROSerialization = null;

            //Deserialization of the byteArray
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerDeserializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericDeserializerSerializationError");
        }

    }

    @Test
    public void testAVROGenericSerializerSerializationDeserialization() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializer();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a EnrichedEventBody object
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Get schema from EnrichedEventBody class
            Schema schema = ReflectData.get().getSchema(EnrichedEventBody.class);

            //Transform EnrichedEventBody object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBody2GenericRecord(enrichedEventBody, schema);

            //Serialization of the GenericRecord object
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(genericRecord);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(genericRecordDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the deserialized object
            //The serialization process with genericRecord uses Utf8 class not String class
            EnrichedEventBody enrichedEventBodyDeserialized = SerializationUtils.genericRecord2EnrichedEventBody(genericRecordDeserialized, true);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBody.getMessage(), enrichedEventBodyDeserialized.getMessage(),"The serialization/deserialization process is not correct.");

            Assert.assertEquals(enrichedEventBody.getExtraData().size(), enrichedEventBodyDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBody.getExtraData().keySet();

            for (String keyEnrichedEventBody : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBody.getExtraData().get(keyEnrichedEventBody), enrichedEventBodyDeserialized.getExtraData().get(keyEnrichedEventBody), "The serialization/deserialization process is not correct.");
            }

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationDeserialization");
        }
    }





    @Test
    public void testAVROGenericSerializerWithoutSchemaFromSerializerFactoryGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer from factory
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializerGeneric();
            serializationBean.setSchema(null);
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerWithoutSchemaFromSerializerFactory");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerWithoutSchemaFromSerializerFactoryGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testAVROGenericSerializerSerializationErrorGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializerGeneric();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric object (with EnrichedEventBodyExtraData Class) (this class doesn't implement required interface)
            String message = "hello";
            EnrichedEventBodyExtraData enrichedEventBodyExtraData = getEnrichedEventBodyExtraData();
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric(enrichedEventBodyExtraData, message, EnrichedEventBodyExtraData.class);

            //Serialize EnrichedEventBody object (EnrichedEventBody class doesn't implement necessary interface)
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(enrichedEventBodyGeneric);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationErrorGenericWithEnrichedEventBodyExtraData");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationErrorGenericWithEnrichedEventBodyExtraData");
        }

    }


    @Test
    public void testAVROGenericSerializerDeserializationErrorWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializerGeneric();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a null byteArray
            byte[] byteArrayAVROSerialization = null;

            //Deserialization of the byteArray
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROGenericSerializerDeserializationErrorWithEnrichedEventBodyExtraData");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerDeserializationErrorWithEnrichedEventBodyExtraData");
        }

    }


    @Test
    public void testAVROGenericSerializerSerializationDeserializationNotEnrichedWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializerGeneric();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a not enriched EnrichedEventBodyGeneric object (with EnrichedEventBodyExtraData Class)
            String message = "hello";
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = EnrichedEventBodyGeneric.createFromEventBody(message.getBytes(), false, EnrichedEventBodyExtraData.class, avroGenericSerializer);

            //Get schema from EnrichedEventBody class
            Schema schema = getSchemaGenericFromFile();

            //Transform EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBodyGeneric2GenericRecord(enrichedEventBodyGeneric, schema);

            //genericRecord.get
            //Serialization of the GenericRecord object
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(genericRecord);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(genericRecordDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the de serialized object
            //The serialization process with genericRecord uses Utf8 class not String class
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = SerializationUtils.genericRecord2EnrichedEventBodyGeneric(genericRecordDeserialized, true, EnrichedEventBodyExtraData.class);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationDeserializationNotEnrichedWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testAVROGenericSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializerGeneric();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric object (with EnrichedEventBodyExtraData Class)
            String message = "hello";
            EnrichedEventBodyExtraData enrichedEventBodyExtraData = getEnrichedEventBodyExtraData();
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric(enrichedEventBodyExtraData, message, EnrichedEventBodyExtraData.class);

            //Get schema from EnrichedEventBody class
            Schema schema = getSchemaGenericFromFile();

            //Transform EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBodyGeneric2GenericRecord(enrichedEventBodyGeneric, schema);

            //Serialization of the GenericRecord object
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(genericRecord);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(genericRecordDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the de serialized object
            //The serialization process with genericRecord uses Utf8 class not String class
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = SerializationUtils.genericRecord2EnrichedEventBodyGeneric(genericRecordDeserialized, true, EnrichedEventBodyExtraData.class);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testAVROGenericSerializerSerializationDeserializationGenericWithHashMap() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROGenericSerializerGenericHashMap();
            AVROGenericSerializer avroGenericSerializer = getAVROGenericSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric object (with HashMap Class)
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();

            //Get class of Extradata
            Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

            //Get schema from serializer
            Schema schema = avroGenericSerializer.getSchema();

            //Transform EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> object to GenericRecord object
            GenericRecord genericRecord = SerializationUtils.enrichedEventBodyGeneric2GenericRecord(enrichedEventBodyGeneric, schema);

            //Serialization of the GenericRecord object
            byte[] byteArrayAVROSerialization = avroGenericSerializer.toBytes(genericRecord);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            GenericRecord genericRecordDeserialized = (GenericRecord) avroGenericSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(genericRecordDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the de serialized object
            //The serialization process with genericRecord uses Utf8 class not String class
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGenericDeserialized = SerializationUtils.genericRecord2EnrichedEventBodyGeneric(genericRecordDeserialized, true, clazz);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");


            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().size(), enrichedEventBodyGenericDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBodyGeneric.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyGenericDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROGenericSerializerSerializationDeserializationGenericWithHashMap");
        }
    }

}
