package org.keedio.flume.interceptor.enrichment.serialization;

import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.exception.SerializationException;
import org.keedio.flume.interceptor.enrichment.serialization.json.JSONSerializer;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializerFactoryTest extends SerializerAbstractTest{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SerializerFactoryTest.class);

    @Test
    public void testCreateNullSerializer() {

        try {
             SerializerFactory serializerFactory = getSerializerFactory();
            serializerFactory.getSerializer(null);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateNullSerializer");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateNullSerializer");
        }
    }



    @Test
    public void testCreateEmptySerializer() {

        try {
            SerializationBean serializationEmptyBean = createEmptySerializationBean();
            SerializerFactory serializerFactory = getSerializerFactory();
            serializerFactory.getSerializer(serializationEmptyBean);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateEmptySerializer");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateEmptySerializer");
        }
    }


    @Test
    public void testCreateUnknownSerializer() {

        try {
            SerializationBean serializationBeanUnknownSerializer = createSerializationBeanUnknownSerializer();
            SerializerFactory serializerFactory = getSerializerFactory();
            serializerFactory.getSerializer(serializationBeanUnknownSerializer);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateUnknownSerializer");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateUnknownSerializer");
        }
    }


    @Test
    public void testCreateJSONSerializer() {

        try {
            SerializationBean serializationBeanJSONSerializer = createSerializationBeanJSONSerializer();
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanJSONSerializer);

            //Serializer must be JSONSerializer
            String serializerClazz = serializer.getClass().getCanonicalName();
            String JSONSerializerClazz = JSONSerializer.class.getCanonicalName();

            Assert.assertEquals(serializerClazz, JSONSerializerClazz,"The class of the serializer is not correct");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateJSONSerializer");
        }
    }



    @Test
    public void testCreateAVROGenericSerializerWithoutSchema() {

        try {
            SerializationBean serializationBeanAVROGenericSerializer = createSerializationBeanAVROGenericSerializer();
            serializationBeanAVROGenericSerializer.setSchema(null);
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROGenericSerializer);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateAVROGenericSerializerWithoutSchema");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROGenericSerializerWithoutSchema");
        }
    }



    @Test
    public void testCreateAVROGenericSerializer() {

        try {
            SerializationBean serializationBeanAVROGenericSerializer = createSerializationBeanAVROGenericSerializer();
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROGenericSerializer);

            //Serializer must be AVROGenericSerializer
            String serializerClazz = serializer.getClass().getCanonicalName();
            String avroGenericSerializerClazz = AVROGenericSerializer.class.getCanonicalName();

            Assert.assertEquals(serializerClazz, avroGenericSerializerClazz,"The class of the serializer is not correct");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROGenericSerializer");
        }
    }



    @Test
    public void testCreateAVROSpecificSerializerWithoutClass() {

        try {
            SerializationBean serializationBeanAVROSpecificSerializer = createSerializationBeanAVROSpecificSerializer(false);
            serializationBeanAVROSpecificSerializer.setClazz(null);
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROSpecificSerializer);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateAVROSpecificSerializerWithoutClass");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROSpecificSerializerWithoutClass");
        }
    }

    //The class doesn't implement SpecificRecord AVRO interface
    @Test
    public void testCreateAVROSpecificSerializerIncorrectClass() {

        try {
            SerializationBean serializationBeanAVROSpecificSerializer = createSerializationBeanAVROSpecificSerializer(false);
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROSpecificSerializer);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateAVROSpecificSerializerIncorrectClass");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROSpecificSerializerIncorrectClass");
        }
    }


    //The class implements SpecificRecord AVRO interface
    @Test
    public void testCreateAVROSpecificSerializerCorrectClass() {

        try {
            SerializationBean serializationBeanAVROSpecificSerializer = createSerializationBeanAVROSpecificSerializer(true);
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROSpecificSerializer);

            //Serializer must be AVROSpecificSerializer
            String serializerClazz = serializer.getClass().getCanonicalName();
            String avroSpecificSerializerClazz = AVROSpecificSerializer.class.getCanonicalName();

            Assert.assertEquals(serializerClazz, avroSpecificSerializerClazz,"The class of the serializer is not correct");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROSpecificSerializerCorrectClass");
        }
    }

    @Test
    public void testCreateAVROReflectiveSerializerWithoutSchema() {

        try {
            SerializationBean serializationBeanAVROReflectiveSerializer = createSerializationBeanAVROReflectiveSerializer();
            serializationBeanAVROReflectiveSerializer.setSchema(null);
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROReflectiveSerializer);

            //SerializationException has been thrown
            Assert.fail("Unexpected exception in testCreateAVROReflectiveSerializerWithoutSchema");
        } catch (SerializationException e) {
            //Is expected exception
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROReflectiveSerializerWithoutSchema");
        }
    }



    @Test
    public void testCreateAVROReflectiveSerializer() {

        try {
            SerializationBean serializationBeanAVROReflectiveSerializer = createSerializationBeanAVROReflectiveSerializer();
            SerializerFactory serializerFactory = getSerializerFactory();
            Serializer serializer = serializerFactory.getSerializer(serializationBeanAVROReflectiveSerializer);

            //Serializer must be AVROSpecificSerializer
            String serializerClazz = serializer.getClass().getCanonicalName();
            String avroReflectiveSerializerClazz = AVROReflectiveSerializer.class.getCanonicalName();

            Assert.assertEquals(serializerClazz, avroReflectiveSerializerClazz,"The class of the serializer is not correct");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception in testCreateAVROReflectiveSerializer");
        }
    }

}
