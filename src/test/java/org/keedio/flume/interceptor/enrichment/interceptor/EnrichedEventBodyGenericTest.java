package org.keedio.flume.interceptor.enrichment.interceptor;


import org.keedio.flume.interceptor.enrichment.serialization.JSONStringSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.Serializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.json.JSONSerializer;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Set;


/**
 * Created by PC on 10/06/2016.
 */
public class EnrichedEventBodyGenericTest extends EnrichedEventBodyGenericAbstractTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichedEventBodyGenericTest.class);

    @Test
    public void testCreateEmptyEnrichedEventBodyGeneric() {

        try {
            String message = "hello";

            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>();

            Assert.assertNotNull(enrichedEventBodyGeneric);
            Assert.assertNull(enrichedEventBodyGeneric.getMessage(), "The built object is not correct");
            Assert.assertNull(enrichedEventBodyGeneric.getExtraData(), "The built object is not correct");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptyEnrichedEventBodyGeneric");
        }
    }



    @Test
    public void testCreateEmptyEnrichedEventBodyGenericWithNullClass() {

        try {
            String message = "hello";

            EnrichedEventBodyGeneric enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric(message, null);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNull(enrichedEventBodyGeneric.getExtraData());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptyEnrichedEventBodyGenericWithStringClass");
        }

    }



    @Test
    public void testCreateEmptyEnrichedEventBodyGenericWithStringClass() {

        try {
            String message = "hello";

            EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<String>(message, String.class);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNotNull(enrichedEventBodyGeneric.getExtraData());
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData() instanceof String, "The built object is not correct");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptyEnrichedEventBodyGenericWithStringClass");
        }

    }



    @Test
    public void testCreateEmptyEnrichedEventBodyGenericWithPOJOClass() {

        try {
            String message = "hello";

            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(message, EnrichedEventBodyExtraData.class);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNotNull(enrichedEventBodyGeneric.getExtraData());
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData() instanceof EnrichedEventBodyExtraData, "The built object is not correct");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptyEnrichedEventBodyGenericWithPOJOClass");
        }

    }


    @Test
    public void testCreateEmptyEnrichedEventBodyGenericWithHashMapClass() {
        try {
            String message = "hello";

            HashMap<String, String> map = new HashMap<String, String>();
            Class clazz = map.getClass();

            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(message, clazz);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNotNull(enrichedEventBodyGeneric.getExtraData());
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData() instanceof HashMap, "The built object is not correct");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptyEnrichedEventBodyGenericWithHashMapClass");
        }

    }


    @Test
    public void testCreateEnrichedEventBodyGenericWithStringClass() {
        try {
            String message = "hello";
            String extraData = "extraData";

            EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<String>(extraData, message, String.class);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNotNull(enrichedEventBodyGeneric.getExtraData());
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData() instanceof String, "The built object is not correct");

            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), message, "The built object is not correct");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData(), extraData, "The built object is not correct");
    } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("Unexpected exception in testCreateEnrichedEventBodyGenericWithStringClass");
    }

    }


    @Test
    public void testCreateEnrichedEventBodyGenericWithPOJOClass() {
        try {
            String message = "hello";
            EnrichedEventBodyExtraData extraData = getEnrichedEventBodyExtraData();

            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(extraData, message, EnrichedEventBodyExtraData.class);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNotNull(enrichedEventBodyGeneric.getExtraData());
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData() instanceof EnrichedEventBodyExtraData, "The built object is not correct");

            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), message, "The built object is not correct");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData(), extraData, "The built object is not correct");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEnrichedEventBodyGenericWithPOJOClass");
        }

    }

    @Test
    public void testCreateEnrichedEventBodyGenericWithHashMapClass() {
        try {
            String message = "hello";

            HashMap<String, String> mapExtraData = getEnrichedEventBodyExtraDataHashMap();
            Class clazz = mapExtraData.getClass();

            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(mapExtraData, message, clazz);

            Assert.assertNotNull(enrichedEventBodyGeneric.getMessage());
            Assert.assertNotNull(enrichedEventBodyGeneric.getExtraData());
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData() instanceof HashMap, "The built object is not correct");

            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), message, "The built object is not correct");
            Assert.assertTrue(enrichedEventBodyGeneric.getExtraData().size() > 0, "The built object is not correct");


            for (String keyMap : mapExtraData.keySet()) {
                Assert.assertEquals(mapExtraData.get(keyMap),enrichedEventBodyGeneric.getExtraData().get(keyMap),"The built object is not correct");
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptyEnrichedEventBodyGenericWithHashMapClass");
        }

    }


    @Test
    public void testToStringEmptyEnrichedEventBodyGeneric() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>();
        String toString = enrichedEventBodyGeneric.toString();
        try {
            String toJson = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            Assert.assertEquals(toString, toJson);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }

    @Test
    public void testToStringEmptyEnrichedEventBodyGenericWithStringClass() {

        String message = "hello";
        String extraData = "extraData";

        EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<String>(extraData, message, String.class);
        String toString = enrichedEventBodyGeneric.toString();
        try {
            String toJson = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            Assert.assertEquals(toString, toJson);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }


    @Test
    public void testToStringEmptyEnrichedEventBodyGenericWithPOJOClass() {

        String message = "hello";
        EnrichedEventBodyExtraData extraData = getEnrichedEventBodyExtraData();

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(extraData, message, EnrichedEventBodyExtraData.class);
        String toString = enrichedEventBodyGeneric.toString();
        try {
            String toJson = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            Assert.assertEquals(toString, toJson);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }


    @Test
    public void testToStringEmptyEnrichedEventBodyGenericWithHashMapClass() {

        String message = "hello";
        HashMap<String, String> mapExtraData = getEnrichedEventBodyExtraDataHashMap();
        Class clazz = mapExtraData.getClass();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(mapExtraData, message, clazz);
        String toString = enrichedEventBodyGeneric.toString();
        try {
            String toJson = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            Assert.assertEquals(toString, toJson);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }


    @Test
    public void testCreateFromEventBodyEmptyEnrichedEventBodyGenericWithStringClass() {

        EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<String>();

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, String.class);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testCreateFromEventBodyEmptyEnrichedEventBodyGenericWithPOJOClass() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>();

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, EnrichedEventBodyExtraData.class);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testCreateFromEventBodyEmptyEnrichedEventBodyGenericWithHashMapClass() {

        HashMap<String, String> mapExtraData = new HashMap<String, String>();
        Class clazz = mapExtraData.getClass();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<HashMap<String, String>>();

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, clazz);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }



    @Test
    public void testCreateFromEventBodyEmptyEnrichedEventBodyGenericWithoutExtradataWithStringClass() {

        String message = "hello";

        EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<String>(message, String.class);

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, String.class);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testCreateFromEventBodyEmptyEnrichedEventBodyGenericWithoutExtradataWithPOJOClass() {

        String message = "hello";

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(message, EnrichedEventBodyExtraData.class);

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, EnrichedEventBodyExtraData.class);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }



    @Test
    public void testCreateFromEventBodyEmptyEnrichedEventBodyGenericWithoutExtradataWithHashMapClass() {

        String message = "hello";
        HashMap<String, String> mapExtraData = new HashMap<String, String>();
        Class clazz = mapExtraData.getClass();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<HashMap<String, String>>(message,clazz);

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);

            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, clazz);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testCreateFromEventBodyEnrichedEventBodyGenericWithStringClass() {

        String message = "hello";
        String extraData = "extraData";

        EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<String>(extraData, message, String.class);

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            EnrichedEventBodyGeneric<String> newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, String.class);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testCreateFromEventBodyEnrichedEventBodyGenericWithPOJOClass() {

        String message = "hello";
        EnrichedEventBodyExtraData extraData = getEnrichedEventBodyExtraData();

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(extraData, message, EnrichedEventBodyExtraData.class);

        String json;
        try {
           json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
           EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, EnrichedEventBodyExtraData.class);

           Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage), json);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testCreateFromEventBodyEnrichedEventBodyGenericWithHashMapClass() {

        String message = "hello";
        HashMap<String, String> mapExtraData = getEnrichedEventBodyExtraDataHashMap();
        Class clazz = mapExtraData.getClass();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(mapExtraData, message, clazz);

        String json;
        try {
            json = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            EnrichedEventBodyGeneric<HashMap<String, String>> newMessage = EnrichedEventBodyGeneric.createFromEventBody(json.getBytes(), true, clazz);

            Assert.assertEquals(JSONStringSerializer.toJSONString(newMessage).length(), json.length());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testBuildEventBodyEmptyWithStringClass() {

        EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<String>();

        try {
            byte[] arrayBytes = enrichedEventBodyGeneric.buildEventBody();
            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(arrayBytes, true, String.class);

            Assert.assertNotNull(newMessage, "The byte array is not correct");
            String jsonOriginal = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            String jsonNew = JSONStringSerializer.toJSONString(newMessage);

            Assert.assertEquals(jsonOriginal, jsonNew, "The built object is not correct");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }


    @Test
    public void testBuildEventBodyEmptyWithPOJOClass() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>();

        try {
            byte[] arrayBytes = enrichedEventBodyGeneric.buildEventBody();
            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(arrayBytes, true, EnrichedEventBodyExtraData.class);

            Assert.assertNotNull(newMessage, "The byte array is not correct");
            String jsonOriginal = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            String jsonNew = JSONStringSerializer.toJSONString(newMessage);

            Assert.assertEquals(jsonOriginal, jsonNew, "The built object is not correct");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }


    @Test
    public void testBuildEventBodyEmptyWithHashMapClass() {

        HashMap<String, String> mapExtraData = new HashMap<String, String>();
        Class clazz = mapExtraData.getClass();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<HashMap<String, String>>();

        try {
            byte[] arrayBytes = enrichedEventBodyGeneric.buildEventBody();
            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(arrayBytes, true, clazz);

            Assert.assertNotNull(newMessage, "The byte array is not correct");
            String jsonOriginal = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            String jsonNew = JSONStringSerializer.toJSONString(newMessage);

            Assert.assertEquals(jsonOriginal, jsonNew, "The built object is not correct");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }


    @Test
    public void testBuildEventBodyWithStringClass() {

        String message = "hello";
        String extraData = "extraData";

        EnrichedEventBodyGeneric<String> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<String>(extraData, message, String.class);

        try {
            byte[] arrayBytes = enrichedEventBodyGeneric.buildEventBody();
            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(arrayBytes, true, String.class);

            Assert.assertNotNull(newMessage, "The byte array is not correct");
            String jsonOriginal = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            String jsonNew = JSONStringSerializer.toJSONString(newMessage);

            Assert.assertEquals(jsonOriginal, jsonNew, "The built object is not correct");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }



    @Test
    public void testBuildEventBodyWithPOJOClass() {

        String message = "hello";
        EnrichedEventBodyExtraData extraData = getEnrichedEventBodyExtraData();

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = new  EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(extraData, message, EnrichedEventBodyExtraData.class);


        try {
            byte[] arrayBytes = enrichedEventBodyGeneric.buildEventBody();
            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(arrayBytes, true, EnrichedEventBodyExtraData.class);

            Assert.assertNotNull(newMessage, "The byte array is not correct");
            String jsonOriginal = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            String jsonNew = JSONStringSerializer.toJSONString(newMessage);

            Assert.assertEquals(jsonOriginal, jsonNew, "The built object is not correct");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }



    @Test
    public void testBuildEventBodyWithHashMapClass() {

        String message = "hello";
        HashMap<String, String> mapExtraData = getEnrichedEventBodyExtraDataHashMap();
        Class clazz = mapExtraData.getClass();

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(mapExtraData, message, clazz);

        try {
            byte[] arrayBytes = enrichedEventBodyGeneric.buildEventBody();
            EnrichedEventBodyGeneric newMessage = EnrichedEventBodyGeneric.createFromEventBody(arrayBytes, true, clazz);

            Assert.assertNotNull(newMessage, "The byte array is not correct");
            String jsonOriginal = JSONStringSerializer.toJSONString(enrichedEventBodyGeneric);
            String jsonNew = JSONStringSerializer.toJSONString(newMessage);

            Assert.assertEquals(jsonOriginal.length(), jsonNew.length(), "The built object is not correct");

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }





    @Test
    public void testBuildBodyAVROGenericSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Generic Serializer
            AVROGenericSerializer avroSerializer = getAVROGenericSerializer();

            //Serialization AVRO of the object
            byte[] byteArrayEnrichedEventBodyGeneric = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            Assert.assertTrue(byteArrayEnrichedEventBodyGeneric.length > 0, "The process of AVRO generic serialization is not correct");

            System.out.println("byte array length: " + byteArrayEnrichedEventBodyGeneric.length);


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testBuildBodyAVROGenericSerializer");
        }
    }



    @Test
    public void testBuildBodyAVROSpecificSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Generic Serializer
            AVROSpecificSerializer avroSerializer = getAVROSpecificSerializer();

            //Serialization AVRO of the object
            byte[] byteArrayEnrichedEventBodyGeneric = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            Assert.assertTrue(byteArrayEnrichedEventBodyGeneric.length > 0, "The process of AVRO generic serialization is not correct");

            System.out.println("byte array length: " + byteArrayEnrichedEventBodyGeneric.length);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testBuildBodyAVROGenericSerializer");
        }
    }


    @Test
    public void testBuildBodyAVROReflectiveSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Reflective Serializer
            AVROReflectiveSerializer avroSerializer = getAVROReflectiveSerializer();

            //Serialization AVRO of the object
            byte[] byteArrayEnrichedEventBodyGeneric = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            Assert.assertTrue(byteArrayEnrichedEventBodyGeneric.length > 0, "The process of AVRO generic serialization is not correct");

            System.out.println("byte array length: " + byteArrayEnrichedEventBodyGeneric.length);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testBuildBodyAVROReflectiveSerializer");
        }
    }


    @Test
    public void testBuildBodyJSONSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get JSON Serializer
            JSONSerializer jsonSerializer = getJSONSerializer();

            //Serialization JSON of the object
            byte[] byteArrayEnrichedEventBodyGeneric = enrichedEventBodyGeneric.buildEventBody(jsonSerializer);

            Assert.assertTrue(byteArrayEnrichedEventBodyGeneric.length > 0, "The process of JSON serialization is not correct");

            System.out.println("byte array length: " + byteArrayEnrichedEventBodyGeneric.length);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testBuildBodyJSONSerializer");
        }
    }


    @Test
    public void testBuildBodyNullSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Serialization JSON of the object
            byte[] byteArrayEnrichedEventBodyGeneric = enrichedEventBodyGeneric.buildEventBody(null);

            Assert.assertTrue(byteArrayEnrichedEventBodyGeneric.length > 0, "The process of JSON serialization is not correct");

            System.out.println("byte array length: " + byteArrayEnrichedEventBodyGeneric.length);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testBuildBodyNullSerializer");
        }
    }


    @Test
    public void testCreateFromEventBodyAVROGenericSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Generic Serializer
            AVROGenericSerializer avroSerializer = getAVROGenericSerializer();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, EnrichedEventBodyExtraData.class, avroSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyAVROGenericSerializer");
        }
    }


    @Test
    public void testCreateFromEventBodyAVROGenericSerializerHashMap() {

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();
        Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

        try {
            //Get AVRO Generic Serializer
            AVROGenericSerializer avroSerializer = getAVROGenericSerializerHashMap();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, clazz, avroSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().size(), enrichedEventBodyGenericDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBodyGeneric.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyGenericDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyAVROGenericSerializerHashMap");
        }
    }


    @Test
    public void testCreateFromEventBodyAVROSpecificSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Generic Serializer
            AVROSpecificSerializer avroSerializer = getAVROSpecificSerializer();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, EnrichedEventBodyExtraData.class, avroSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyAVROSpecificSerializer");
        }
    }


    @Test
    public void testCreateFromEventBodyAVROSpecificSerializerHashMap() {

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();
        Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

        try {
            //Get AVRO Generic Serializer
            AVROSpecificSerializer avroSerializer = getAVROSpecificSerializerHashMap();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, clazz, avroSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().size(), enrichedEventBodyGenericDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBodyGeneric.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyGenericDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyAVROSpecificSerializerHashMap");
        }
    }





    @Test
    public void testCreateFromEventBodyAVROReflectiveSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Generic Serializer
            AVROReflectiveSerializer avroSerializer = getAVROReflectiveSerializer();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, EnrichedEventBodyExtraData.class, avroSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyAVROGenericSerializer");
        }
    }


    @Test
    public void testCreateFromEventBodyAVROReflectiveSerializerHashMap() {

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();
        Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

        try {
            //Get AVRO Generic Serializer
            AVROReflectiveSerializer avroSerializer = getAVROReflectiveHashMap();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(avroSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, clazz, avroSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().size(), enrichedEventBodyGenericDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBodyGeneric.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyGenericDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyAVROReflectiveSerializerHashMap");
        }
    }


    @Test
    public void testCreateFromEventBodyJSONSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Get AVRO Generic Serializer
            JSONSerializer jsonSerializer = getJSONSerializer();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(jsonSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, EnrichedEventBodyExtraData.class, jsonSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyJSONSerializer");
        }
    }


    @Test
    public void testCreateFromEventBodyJSONSerializerHashMap() {

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();
        Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

        try {
            //Get AVRO Generic Serializer
            JSONSerializer jsonSerializer = getJSONSerializerHashMap();

            //Serialization AVRO of the object
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(jsonSerializer);

            //Deserialization of the object
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, clazz, jsonSerializer);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().size(), enrichedEventBodyGenericDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBodyGeneric.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyGenericDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyJSONSerializerHashMap");
        }
    }


    @Test
    public void testCreateFromEventBodyNullSerializer() {

        EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = createEnrichedEventBodyGeneric();

        try {
            //Serialization of the object (by default JSON Serialization)
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(null);

            //Deserialization of the object
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, EnrichedEventBodyExtraData.class, null);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTopic(), enrichedEventBodyGenericDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getTimestamp(), enrichedEventBodyGenericDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getSha1Hex(), enrichedEventBodyGenericDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFilePath(), enrichedEventBodyGenericDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getFileName(), enrichedEventBodyGenericDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getLineNumber(), enrichedEventBodyGenericDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().getType(), enrichedEventBodyGenericDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyNullSerializer");
        }
    }


    @Test
    public void testCreateFromEventBodyNullSerializerHashMap() {

        EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGeneric = createEnrichedEventBodyGenericHashMap();
        Class clazz = enrichedEventBodyGeneric.getExtraData().getClass();

        try {
            //Serialization of the object (by default JSON Serialization)
            byte[] payload = enrichedEventBodyGeneric.buildEventBody(null);

            //Deserialization of the object
            EnrichedEventBodyGeneric<HashMap<String, String>> enrichedEventBodyGenericDeserialized = EnrichedEventBodyGeneric.createFromEventBody(payload, true, clazz, null);

            //Test equality of the original object and the deserialized object
            Assert.assertEquals(enrichedEventBodyGeneric.getMessage(), enrichedEventBodyGenericDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().size(), enrichedEventBodyGenericDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBody = enrichedEventBodyGeneric.getExtraData().keySet();

            for (String keyEnrichedEventBod : keysetEnrichedEventBody) {
                Assert.assertEquals(enrichedEventBodyGeneric.getExtraData().get(keyEnrichedEventBod), enrichedEventBodyGenericDeserialized.getExtraData().get(keyEnrichedEventBod), "The serialization/deserialization process is not correct.");
            }

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyNullSerializerHashMap");
        }
    }


    @Test
    public void testCreateFromEventBodyNotEnriched() {

        String expectedCharset = StandardCharsets.UTF_8.name();

        try {

            Path path = Paths.get("src/test/resources/notUTFString.txt");
            byte[] payload = Files.readAllBytes(path);

            //Detect charset
            String originalCharset = detectCharset(payload);

            //Deserializarion and serialization of the not enriched object
            Serializer theSerializer = getAVROGenericSerializer();

            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> message = EnrichedEventBodyGeneric.createFromEventBody(payload, false, EnrichedEventBodyExtraData.class, theSerializer);
            byte[] output = message.buildEventBody(theSerializer);
            String charsetAfter = detectCharset(output);
            //The charset has not been modified
            Assert.assertEquals(charsetAfter, originalCharset, "The charset is not correct");


            //Serialization of the not enriched object
            theSerializer = getAVROSpecificSerializer();
            output = message.buildEventBody(theSerializer);
            charsetAfter = detectCharset(output);
            //The charset has not been modified
            Assert.assertEquals(charsetAfter, originalCharset, "The charset is not correct");


            //Serialization of the not enriched object
            theSerializer = getAVROReflectiveSerializer();
            output = message.buildEventBody(theSerializer);
            charsetAfter = detectCharset(output);
            //The charset has not been modified
            Assert.assertEquals(charsetAfter, originalCharset, "The charset is not correct");


            //Serialization of the not enriched object
            theSerializer = getJSONSerializer();
            output = message.buildEventBody(theSerializer);
            charsetAfter = detectCharset(output);
            //The charset has been modified to UTF-8
            Assert.assertEquals(charsetAfter, expectedCharset, "The charset is not correct");

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateFromEventBodyNotEnriched");
        }
    }



}
