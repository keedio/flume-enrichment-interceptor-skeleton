package org.keedio.flume.interceptor.enrichment.interceptor;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.type.JavaType;
import org.keedio.flume.interceptor.enrichment.serialization.JSONStringSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.Serializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROGenericSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROReflectiveSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.avro.serializers.AVROSpecificSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.json.JSONSerializer;
import org.keedio.flume.interceptor.enrichment.serialization.utils.SerializationUtils;
import org.mozilla.universalchardet.UniversalDetector;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by PC on 10/06/2016.
 */
public class EnrichedEventBodyGeneric<T> {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EnrichedEventBody.class);
    private static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();


    private T extraData;
    private String message;


    @JsonCreator
    public EnrichedEventBodyGeneric(@JsonProperty("extraData") T extraData,
                                    @JsonProperty("message") String message,
                                    @JsonProperty("clazz") Class<T> clazz) {
        try {
            if (extraData == null) {
                if (clazz != null) {
                    this.extraData = clazz.newInstance();
                }
            } else {
                this.extraData = extraData;
            }
            this.message = message;
        } catch (IllegalAccessException e) {
            logger.error("Exception in EnrichedEventBodyGeneric constructor");
        } catch (InstantiationException e) {
            logger.error("Exception in EnrichedEventBodyGeneric constructor");
        }

    }


    public EnrichedEventBodyGeneric(String message, Class<T> clazz) {

        try {
            if (clazz != null) {
                this.extraData = clazz.newInstance();
            }
            this.message = message;

        } catch (IllegalAccessException e) {
            logger.error("Exception in EnrichedEventBodyGeneric constructor");
        } catch (InstantiationException e) {
            logger.error("Exception in EnrichedEventBodyGeneric constructor");
        }
    }

    public EnrichedEventBodyGeneric() {}



    public T getExtraData() {
        return extraData;
    }

    public void setExtraData(T extraData) {
        this.extraData = extraData;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    @Override
    public String toString() {
        String s = null;
        try {
            s = JSONStringSerializer.toJSONString(this);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return s;
    }


    public static <T> EnrichedEventBodyGeneric createFromEventBody(byte[] payload, boolean isEnriched, Class<T> clazz) throws IOException {

        EnrichedEventBodyGeneric enrichedEventBodyGeneric;

        if (isEnriched) {
            JavaType javaType = JSONStringSerializer.getJavaType(EnrichedEventBodyGeneric.class, clazz);
            enrichedEventBodyGeneric = (EnrichedEventBodyGeneric) JSONStringSerializer.fromBytes(payload, javaType);
        } else {
            // Detecting payload charset
            UniversalDetector detector = new UniversalDetector(null);
            detector.handleData(payload, 0, payload.length);
            detector.dataEnd();
            String charset = detector.getDetectedCharset();
            detector.reset();

            if (charset == null) {
                charset = DEFAULT_CHARSET;
            }
           enrichedEventBodyGeneric = new EnrichedEventBodyGeneric(new String(payload, charset), clazz);
        }

        return enrichedEventBodyGeneric;
    }


    public byte[] buildEventBody() throws IOException {
        return JSONStringSerializer.toBytes(this);
    }

    public static <T> EnrichedEventBodyGeneric createFromEventBody(byte[] payload, boolean isEnriched, Class<T> clazz, Serializer serializer) throws IOException {

        EnrichedEventBodyGeneric enrichedBodyGeneric;

        if (isEnriched) {

            if (serializer instanceof AVROGenericSerializer) {
                GenericRecord genericRecord = (GenericRecord) serializer.toObject(payload);
                enrichedBodyGeneric = SerializationUtils.genericRecord2EnrichedEventBodyGeneric(genericRecord, true, clazz);
            } else if (serializer instanceof AVROSpecificSerializer) {
                //EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = (EnrichedEventBodyGenericAvroString) serializer.toObject(payload);
                //enrichedBodyGeneric = SerializationUtils.enrichedEventBodyGenericAvroString2EnrichedEventBodyGeneric(enrichedEventBodyGenericAvroString);
                Object specificAvroClassObject = serializer.toObject(payload);
                enrichedBodyGeneric = SerializationUtils.specificAvroClass2EnrichedEventBodyGeneric(specificAvroClassObject);
            } else if (serializer instanceof AVROReflectiveSerializer) {
                enrichedBodyGeneric = (EnrichedEventBodyGeneric) serializer.toObject(payload);
            } else if (serializer instanceof JSONSerializer) {
                enrichedBodyGeneric = (EnrichedEventBodyGeneric) serializer.toObject(payload, clazz);
            } else {
                enrichedBodyGeneric = createFromEventBody(payload, isEnriched, clazz);
            }

        } else {
            // Detecting payload charset
            UniversalDetector detector = new UniversalDetector(null);
            detector.handleData(payload, 0, payload.length);
            detector.dataEnd();
            String charset = detector.getDetectedCharset();
            detector.reset();

            if (charset == null) {
                charset = DEFAULT_CHARSET;
            }
            enrichedBodyGeneric = new  EnrichedEventBodyGeneric(new String(payload, charset), clazz);
        }

        return enrichedBodyGeneric;
    }


    public byte[] buildEventBody(Serializer serializer) throws IOException {

        byte[] byteArrayResult = null;

        if (serializer instanceof AVROGenericSerializer) {
            //The class doesn't implements SpecificRecord
            GenericRecord genericRecord = SerializationUtils.enrichedEventBodyGeneric2GenericRecord((EnrichedEventBodyGeneric) this, ((AVROGenericSerializer) serializer).getSchema());
            byteArrayResult =  serializer.toBytes(genericRecord);
        } else if (serializer instanceof AVROSpecificSerializer) {
            //EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = SerializationUtils.enrichedEventBodyGeneric2enrichedEventBodyGenericAvroString((EnrichedEventBodyGeneric) this);
            Object enrichedEventBodyGenericAvroString = SerializationUtils.enrichedEventBodyGeneric2SpecificAvroClass((EnrichedEventBodyGeneric) this);
            byteArrayResult =  serializer.toBytes(enrichedEventBodyGenericAvroString);
        } else if (serializer instanceof AVROReflectiveSerializer) {
            byteArrayResult =  serializer.toBytes((EnrichedEventBodyGeneric) this);
        } else if (serializer instanceof JSONSerializer) {
            byteArrayResult =  serializer.toBytes((EnrichedEventBodyGeneric) this);
        } else {
            byteArrayResult =  buildEventBody();
        }
        return byteArrayResult;
    }

}
