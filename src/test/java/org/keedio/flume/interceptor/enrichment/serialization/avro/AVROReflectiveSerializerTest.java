package org.keedio.flume.interceptor.enrichment.serialization.avro;

import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyExtraData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyGeneric;
import org.keedio.flume.interceptor.enrichment.serialization.SerializationBean;
import org.keedio.flume.interceptor.enrichment.serialization.SerializerAbstractTest;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by PC on 08/06/2016.
 */
public class AVROReflectiveSerializerTest extends SerializerAbstractTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AVROReflectiveSerializerTest.class);


    @Test
    public void testAVROReflectiveSerializerWithoutSchemaFromSerializerFactory() {

        try {

            //Get serializer from factory
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializer();
            serializationBean.setSchema(null);
            AVROReflectiveSerializer avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerWithoutSchemaFromSerializerFactory");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerWithoutSchemaFromSerializerFactory");
        }
    }


    @Test
    public void testAVROReflectiveSerializerWithoutSchema() {

        try {

            //Get serializer directly (not from Serializer factory)
            AVROReflectiveSerializer avroReflectiveSerializer = new AVROReflectiveSerializer(null);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerWithoutSchema");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerWithoutSchema");
        }
    }


    @Test
    public void testAVROReflectiveSerializerSerializationError() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializer();
            AVROReflectiveSerializer avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //Serialize a fake object (the class of the object is not the same that the class of the serializer)
            byte[] byteArrayAVROSerialization = avroReflectiveSerializer.toBytes(createSpecificRecordFakeClass());

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerSerializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerSerializationError");
        }
    }


    @Test
    public void testAVROReflectiveSerializerDeserializationError() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializer();
            AVROReflectiveSerializer avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //Create a null byteArray
            byte[] byteArrayAVROSerialization = null;

            //Deserialization of the byteArray
            EnrichedEventBody enrichedEventBody = (EnrichedEventBody) avroReflectiveSerializer.toObject(byteArrayAVROSerialization);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerDeserializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerDeserializationError");
        }
    }


    @Test
    public void testAVROReflectiveSerializerSerializationDeserialization() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializer();
            //serializationBean.setSchema(ReflectData.get().getSchema(EnrichedEventBody.class).toString());
            AVROReflectiveSerializer<EnrichedEventBody> avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //Create a EnrichedEventBody object
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the EnrichedEventBody object
            byte[] byteArrayAVROSerialization = avroReflectiveSerializer.toBytes(enrichedEventBody);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBody enrichedEventBodyDeserialized = avroReflectiveSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBody.getMessage(), enrichedEventBodyDeserialized.getMessage(),"The serialization/deserialization process is not correct.");

            Assert.assertEquals(enrichedEventBody.getExtraData().size(), enrichedEventBodyDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBody.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBody.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerSerializationDeserialization");
        }
    }


    @Test
    public void testAVROReflectiveSerializerSerializationDeserializationNotEnrichedWithEnrichedEventBodyExtraData() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializerGeneric();
            //serializationBean.setSchema(ReflectData.get().getSchema(EnrichedEventBody.class).toString());
            AVROReflectiveSerializer<EnrichedEventBodyGeneric> avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //Create a not enriched EnrichedEventBodyGeneric object (with EnrichedEventBodyExtraData Class)
            String message = "hello";
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = EnrichedEventBodyGeneric.createFromEventBody(message.getBytes(), false, EnrichedEventBodyExtraData.class, avroReflectiveSerializer);

            //Serialization of the EnrichedEventBody object
            byte[] byteArrayAVROSerialization = avroReflectiveSerializer.toBytes(enrichedEventBodyGeneric);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>  enrichedEventBodyGenericDeserialized = avroReflectiveSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyGenericDeserialized ,"The deserialization process is not correct.");

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
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerSerializationDeserializationNotEnrichedWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testAVROReflectiveSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializerGeneric();
            //serializationBean.setSchema(ReflectData.get().getSchema(EnrichedEventBody.class).toString());
            AVROReflectiveSerializer<EnrichedEventBodyGeneric> avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the EnrichedEventBodyGeneric object
            byte[] byteArrayAVROSerialization = avroReflectiveSerializer.toBytes(enrichedEventBodyGeneric);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>  enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>) avroReflectiveSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyGenericDeserialized ,"The deserialization process is not correct.");

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
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testAVROReflectiveSerializerSerializationDeserializationGenericWithHashMap() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROReflectiveSerializerGenericHashMap();
            AVROReflectiveSerializer<EnrichedEventBodyGeneric> avroReflectiveSerializer = getAVROReflectiveSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> object
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();

            //Serialization of the EnrichedEventBodyGeneric object
            byte[] byteArrayAVROSerialization = avroReflectiveSerializer.toBytes(enrichedEventBodyGeneric);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<HashMap<String, String>>  enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<HashMap<String, String>>) avroReflectiveSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyGenericDeserialized ,"The deserialization process is not correct.");

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
            Assert.fail("Unexpected exception in testAVROReflectiveSerializerSerializationDeserializationGenericWithHashMap");
        }
    }
}
