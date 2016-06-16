package org.keedio.flume.interceptor.enrichment.serialization.json;

import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyExtraData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyGeneric;
import org.keedio.flume.interceptor.enrichment.serialization.JSONStringSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.SerializationBean;
import org.keedio.flume.interceptor.enrichment.serialization.SerializerAbstractTest;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by PC on 06/06/2016.
 */
public class JSONSerializerTest extends SerializerAbstractTest {


    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(JSONSerializerTest.class);


    @Test
    public void testJSONSerializerWithoutClassFromSerializerFactory() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            serializationBean.setClazz(null);
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testJSONSerializerWithoutClassFromSerializerFactory");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerWithoutClassFromSerializerFactory");
        }
    }


    @Test
    public void testJSONSerializerWithoutClass() {

        try {

            //Get serializer directly (not from Serializer factory)
            JSONSerializer jsonSerializer = new JSONSerializer(null);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testJSONSerializerWithoutClass");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerWithoutClass");
        }
    }


    @Test
    public void testJSONSerializerEmptySerialization() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(null);

            //Deserialization of the byte array
            EnrichedEventBody enrichedEventBodyDeserialized = (EnrichedEventBody) jsonSerializer.toObject(byteArray);

            //Test deserialized object is null
            Assert.assertNull(enrichedEventBodyDeserialized,"The serialization/deserialization process is not correct.");



        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToByteArray");
        }
    }


    @Test
    public void testJSONSerializerSerializationToByteArray() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(enrichedEventBody);

            //Check byte array has contents
            Assert.assertTrue(byteArray.length > 0, "The serialization to byte array is not correct");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToByteArray");
        }
    }


    @Test
    public void testJSONSerializerSerializationToObject() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(enrichedEventBody);

            //Deserialization of the byte array
            EnrichedEventBody enrichedEventBodyDeserialized = (EnrichedEventBody) jsonSerializer.toObject(byteArray);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToObject");
        }
    }


    @Test
    public void testJSONSerializerSerializationToJSONString() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
           String jsonString = jsonSerializer.toJSONString(enrichedEventBody);

            //Check String has contents
            Assert.assertTrue(jsonString.length() > 0, "The serialization to JSON string is not correct");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToJSONString");
        }
    }


    @Test
    public void testJSONSerializerSerializationFromJSONString() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
            String jsonString = jsonSerializer.toJSONString(enrichedEventBody);

            //logger.debug("JSON String serialization is: " + jsonString);
            System.out.println("JSON String serialization is: " + jsonString);

            //Deserialization of the byte array
            EnrichedEventBody enrichedEventBodyDeserialized = (EnrichedEventBody) jsonSerializer.fromJSONString(jsonString);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToObject");
        }
    }


    @Test
    public void testSameSerializationToBytes() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(enrichedEventBody);

            //Serialization of the object from JSONStringSerializer
            byte[] byteArraySecond = JSONStringSerializer.toBytes(enrichedEventBody);

            Assert.assertTrue(Arrays.equals(byteArray, byteArraySecond),"The serialization to byte array is not correct");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testSameSerializationToBytes");
        }
    }


    @Test
    public void testSameSerializationToJSONString() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializer();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBody
            EnrichedEventBody enrichedEventBody = createEnrichedEventBody();

            //Serialization of the object
           String jsonString = jsonSerializer.toJSONString(enrichedEventBody);

            //Serialization of the object from JSONStringSerializer
            String jsonStringSecond = JSONStringSerializer.toJSONString(enrichedEventBody);

            Assert.assertEquals(jsonString, jsonStringSecond, "The serialization to JSON String is not correct");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testSameSerializationToJSONSTring");
        }
    }



    @Test
    public void testJSONSerializerEmptySerializationGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(null);

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>) jsonSerializer.toObject(byteArray);

            //Test deserialized object is null
            Assert.assertNull(enrichedEventBodyGenericDeserialized,"The serialization/deserialization process is not correct.");



        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerEmptySerializationGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationToByteArrayGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(enrichedEventBodyGeneric);

            //Check byte array has contents
            Assert.assertTrue(byteArray.length > 0, "The serialization to byte array is not correct");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToByteArrayGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationToObjectGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();


            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(enrichedEventBodyGeneric);

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>) jsonSerializer.toObject(byteArray, EnrichedEventBodyExtraData.class);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToObjectGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationToJSONStringGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the object
            String jsonString = jsonSerializer.toJSONString(enrichedEventBodyGeneric);

            //Check String has contents
            Assert.assertTrue(jsonString.length() > 0, "The serialization to JSON string is not correct");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testJSONSerializerSerializationToJSONStringGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationFromJSONStringGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the object
            String jsonString = jsonSerializer.toJSONString(enrichedEventBodyGeneric);

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>) jsonSerializer.fromJSONString(jsonString, EnrichedEventBodyExtraData.class);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationFromJSONStringGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testSameSerializationToBytesGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the object
            byte[] byteArray = jsonSerializer.toBytes(enrichedEventBodyGeneric);

            //Serialization of the object from JSONStringSerializer
            byte[] byteArraySecond = JSONStringSerializer.toBytes(enrichedEventBodyGeneric);

            Assert.assertTrue(Arrays.equals(byteArray, byteArraySecond),"The serialization to byte array is not correct");


        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testSameSerializationToBytesGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testSameSerializationToJSONStringWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric (with EnrichedEventBodyExtraData)
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the object
            String jsonString = jsonSerializer.toJSONString(enrichedEventBodyGeneric);

            //Serialization of the object from JSONStringSerializer
            String jsonStringSecond = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            Assert.assertEquals(jsonString, jsonStringSecond, "The serialization to JSON String is not correct");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testSameSerializationToJSONStringWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationDeserializatioNotEnrichedWithEnrichedEventBodyExtraData() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a not enriched EnrichedEventBodyGeneric object (with EnrichedEventBodyExtraData Class)
            String message = "hello";
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = EnrichedEventBodyGeneric.createFromEventBody(message.getBytes(), false, EnrichedEventBodyExtraData.class, jsonSerializer);

            //Serialization of the EnrichedEventBodyGeneric object
            byte[] byteArrayJSONSerialization = jsonSerializer.toBytes(enrichedEventBodyGeneric);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayJSONSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>  enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>) jsonSerializer.toObject(byteArrayJSONSerialization, EnrichedEventBodyExtraData.class);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationDeserializatioNotEnrichedWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGeneric();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

            //Serialization of the EnrichedEventBodyGeneric object
            byte[] byteArrayJSONSerialization = jsonSerializer.toBytes(enrichedEventBodyGeneric);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayJSONSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>  enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>) jsonSerializer.toObject(byteArrayJSONSerialization, EnrichedEventBodyExtraData.class);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData");
        }
    }


    @Test
    public void testJSONSerializerSerializationDeserializationGenericWithHashMap() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanJSONSerializerGenericWithHashMap();
            JSONSerializer jsonSerializer = getJSONSerializer(serializationBean);

            //Create a EnrichedEventBodyGeneric object (with HashMap Class)
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();

            //Get class of Extradata
            Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

            //Serialization of the EnrichedEventBodyGeneric object
            byte[] byteArrayJSONSerialization = jsonSerializer.toBytes(enrichedEventBodyGeneric);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayJSONSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGeneric<HashMap<String, String>>  enrichedEventBodyGenericDeserialized = (EnrichedEventBodyGeneric<HashMap<String, String>>) jsonSerializer.toObject(byteArrayJSONSerialization, clazz);

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
            Assert.fail("Unexpected exception in testJSONSerializerSerializationDeserializationGenericWithHashMap");
        }
    }

}
