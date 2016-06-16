package org.keedio.flume.interceptor.enrichment.serialization.avro;


import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyExtraData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyGeneric;
import org.keedio.flume.interceptor.enrichment.serialization.SerializationBean;
import org.keedio.flume.interceptor.enrichment.serialization.SerializerAbstractTest;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyMapAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyGenericAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.keedio.flume.interceptor.enrichment.serialization.utils.SerializationUtils;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * Created by PC on 06/06/2016.
 */
public class AVROSpecificSerializerTest extends SerializerAbstractTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AVROSpecificSerializerTest.class);


    @Test
    public void testAVROSpecificSerializerWithoutClassFromSerializerFactory() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializer(false);
            serializationBean.setClazz(null);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROSpecificSerializerWithoutClassFromSerializerFactory");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerWithoutClassFromSerializerFactory");
        }
    }


    @Test
    public void testAVROSpecificSerializerWithoutClass() {

        try {

            //Get serializer directly (not from Serializer factory)
            AVROSpecificSerializer avroSpecificSerializer = new AVROSpecificSerializer(null);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROSpecificSerializerWithoutClass");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerWithoutClass");
        }
    }


    @Test
    public void testAVROSpecificSerializerSerializationError() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializer(true);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //Serialize a fake object (the class of the object is not the same that the class of the serializer)
            byte[] byteArrayAVROSerialization = avroSpecificSerializer.toBytes(createSpecificRecordFakeClass());

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROSpecificSerializerSerializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerSerializationError");
        }
    }



    @Test
    public void testAVROSpecificSerializerDeserializationError() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializer(true);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //Create a null byteArray
            byte[] byteArrayAVROSerialization = null;

            //Deserialization of the byteArray
            EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString = (EnrichedEventBodyMapAvroString) avroSpecificSerializer.toObject(byteArrayAVROSerialization);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROSpecificSerializerDeserializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerDeserializationError");
        }
    }


    @Test
    public void testAVROSpecificSerializerSerializationDeserialization() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializer(true);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //Create a EnrichedEventBodyMapAvroString object
            EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString = createEnrichedEventBodyMapAvroString();

            //Serialization of the EnrichedEventBodyMapAvroString object
            byte[] byteArrayAVROSerialization = avroSpecificSerializer.toBytes(enrichedEventBodyMapAvroString);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroStringDeserialized = (EnrichedEventBodyMapAvroString) avroSpecificSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyMapAvroStringDeserialized,"The deserialization process is not correct.");

            //Test equality of the original object and the deserialized object
            //The class generated by avro-tools (from the schema has been created with String not Utf8)
            Assert.assertEquals(enrichedEventBodyMapAvroString.getMessage(), enrichedEventBodyMapAvroStringDeserialized.getMessage(),"The serialization/deserialization process is not correct.");

            Assert.assertEquals(enrichedEventBodyMapAvroString.getExtraData().size(), enrichedEventBodyMapAvroStringDeserialized.getExtraData().size(),"The serialization/deserialization process is not correct.");

            Set<String> keysetEnrichedEventBodyAvroString = enrichedEventBodyMapAvroString.getExtraData().keySet();

            for (String keyEnrichedEventBodyAvroString : keysetEnrichedEventBodyAvroString) {
                Assert.assertEquals(enrichedEventBodyMapAvroString.getExtraData().get(keyEnrichedEventBodyAvroString), enrichedEventBodyMapAvroStringDeserialized.getExtraData().get(keyEnrichedEventBodyAvroString), "The serialization/deserialization process is not correct.");
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerSerializationDeserialization");
        }
    }



    @Test
    public void testAVROSpecificSerializerWithoutClassFromSerializerFactoryGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializerGeneric(false);
            serializationBean.setClazz(null);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROSpecificSerializerWithoutClassFromSerializerFactoryGenericWithEnrichedEventBodyExtraData");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerWithoutClassFromSerializerFactoryGenericWithEnrichedEventBodyExtraData");
        }
    }





    @Test
    public void testAVROSpecificSerializerDeserializationErrorGenericWithEnrichedEventBodyExtraData() {

        try {

            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializerGeneric(true);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //Create a null byteArray
            byte[] byteArrayAVROSerialization = null;

            //Deserialization of the byteArray
            EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = (EnrichedEventBodyGenericAvroString) avroSpecificSerializer.toObject(byteArrayAVROSerialization);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testAVROSpecificSerializerDeserializationError");

        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerDeserializationError");
        }
    }




    @Test
    public void testAVROSpecificSerializerSerializationDeserializationNotEnrichedWithEnrichedEventBodyExtraData() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializerGeneric(true);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //Create a not enriched EnrichedEventBodyGeneric object (with EnrichedEventBodyExtraData Class)
            String message = "hello";
            EnrichedEventBodyGeneric<EnrichedEventBodyExtraData> enrichedEventBodyGeneric = EnrichedEventBodyGeneric.createFromEventBody(message.getBytes(), false, EnrichedEventBodyExtraData.class, avroSpecificSerializer);
           // EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = SerializationUtils.enrichedEventBodyGeneric2enrichedEventBodyGenericAvroString(enrichedEventBodyGeneric);
            Object specificAvroClass = SerializationUtils.enrichedEventBodyGeneric2SpecificAvroClass(enrichedEventBodyGeneric);
            EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = (EnrichedEventBodyGenericAvroString) specificAvroClass;

            //Serialization of the EnrichedEventBodyMapAvroString object
            //byte[] byteArrayAVROSerialization = avroSpecificSerializer.toBytes(enrichedEventBodyGenericAvroString);
            byte[] byteArrayAVROSerialization = avroSpecificSerializer.toBytes(enrichedEventBodyGenericAvroString);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroStringDeserialized = (EnrichedEventBodyGenericAvroString) avroSpecificSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyGenericAvroStringDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the deserialized object
            //The class generated by avro-tools (from the schema has been created with String not Utf8)
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getMessage(), enrichedEventBodyGenericAvroStringDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getTopic(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getTimestamp(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getSha1Hex(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getFilePath(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getFileName(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getLineNumber(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getType(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData");
        }
    }



    @Test
    public void testAVROSpecificSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData() {

        try {
            //Get serializer
            SerializationBean serializationBean = createSerializationBeanAVROSpecificSerializerGeneric(true);
            AVROSpecificSerializer avroSpecificSerializer = getAVROSpecificSerializer(serializationBean);

            //Create a EnrichedEventBodyGenericAvroString object
            EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = createEnrichedEventBodyGenericAvroString();

            //Serialization of the EnrichedEventBodyGenericAvroString object
            byte[] byteArrayAVROSerialization = avroSpecificSerializer.toBytes(enrichedEventBodyGenericAvroString);

            //Verify that the serialization has contents
            Assert.assertTrue(byteArrayAVROSerialization.length > 0 ,"The serialization process is not correct.");

            //Deserialization of the byte array
            EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroStringDeserialized = (EnrichedEventBodyGenericAvroString) avroSpecificSerializer.toObject(byteArrayAVROSerialization);

            //Verify that the deserialization has been done
            Assert.assertNotNull(enrichedEventBodyGenericAvroStringDeserialized ,"The deserialization process is not correct.");

            //Test equality of the original object and the deserialized object
            //The class generated by avro-tools (from the schema has been created with String not Utf8)
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getMessage(), enrichedEventBodyGenericAvroStringDeserialized.getMessage(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getTopic(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getTopic(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getTimestamp(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getTimestamp(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getSha1Hex(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getSha1Hex(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getFilePath(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getFilePath(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getFileName(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getFileName(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getLineNumber(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getLineNumber(),"The serialization/deserialization process is not correct.");
            Assert.assertEquals(enrichedEventBodyGenericAvroString.getExtraData().getType(), enrichedEventBodyGenericAvroStringDeserialized.getExtraData().getType(),"The serialization/deserialization process is not correct.");


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testAVROSpecificSerializerSerializationDeserializationGenericWithEnrichedEventBodyExtraData");
        }
    }


}
