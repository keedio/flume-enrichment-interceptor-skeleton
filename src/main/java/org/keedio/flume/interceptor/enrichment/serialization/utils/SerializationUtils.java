package org.keedio.flume.interceptor.enrichment.serialization.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyExtraData;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBodyGeneric;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyMapAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyExtraDataAvroString;
import org.keedio.flume.interceptor.enrichment.serialization.avro.specific.EnrichedEventBodyGenericAvroString;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by PC on 06/06/2016.
 */
public class SerializationUtils {

    /**
     * Metodo que genera un GenericRecord con el contenido del objeto enrichedEventBody
     *
     * @param enrichedEventBody EnrichedEventBody que se convierte en GenericRecord
     * @param schema Schema con el schema utilizado en la conversion
     *
     * @return GenericRecord con el contenido de EnrichedEventBody
     */
    public static GenericRecord enrichedEventBody2GenericRecord(EnrichedEventBody enrichedEventBody, Schema schema) {

        GenericRecord datum = new GenericData.Record(schema);

        Map<String, String> extraData = enrichedEventBody.getExtraData();
        String message = enrichedEventBody.getMessage();

        datum.put("message", message);
        datum.put("extraData", extraData);

        return datum;
    }

    /**
     * Metodo que genera un EnrichedEventBody con el contenido del objeto GenericRecord
     *
     * @param genericRecord GenericRecord con el contenido a transformar en EnrichedEventBody
     * @param isUTF8Class boolean indicando si la clase de los objetos que componen enericRecord es String o UTF-8
     *
     * @return EnrichedEventBody con el contenido de GenericRecord
     */
    public static EnrichedEventBody genericRecord2EnrichedEventBody(GenericRecord genericRecord, boolean isUTF8Class) {

        EnrichedEventBody enrichedEventBody = null;

        if (!isUTF8Class) {
            String message = (String) genericRecord.get("message");
            HashMap extraData = (HashMap)  genericRecord.get("extraData");

            enrichedEventBody = new EnrichedEventBody(extraData, message);
        } else {
            String message = genericRecord.get("message").toString();
            HashMap extraData = (HashMap)  genericRecord.get("extraData");


            //Convertimos el map (Map<UTF-8, UTF-8>) en Map<String, String>
            enrichedEventBody = new EnrichedEventBody(null, message);
            Map<String,String> mapExtraDataString = enrichedEventBody.getExtraData();

            Set<Map.Entry> setEntry = extraData.entrySet();

            for (Map.Entry entry : setEntry) {
                String keyString = entry.getKey().toString();
                String valueString = entry.getValue().toString();

                mapExtraDataString.put(keyString, valueString);
            }

        }

        return enrichedEventBody;

    }

    /**
     * Metodo que genera escribe un fichero AVRO con el contenido de listGenericRecord
     *
     * @param avroFile File con la referencia al fichero
     * @param schema Schema con el esquema AVRO
     * @param listGenericRecord List con la lista de objetos a escribir
     *
     */
    public static void writeAVROFile(File avroFile, Schema schema, List<GenericRecord> listGenericRecord) throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, avroFile);

        for (GenericRecord genericRecord : listGenericRecord) {
            dataFileWriter.append(genericRecord);
        }

        dataFileWriter.close();
    }


    /**
     * Metodo que genera escribe un fichero AVRO el contenido de genericRecord
     *
     * @param avroFile File con la referencia al fichero
     * @param schema Schema con el esquema AVRO
     * @param genericRecord GenericRecord con el objeto a serializar en fichero
     *
     */
    public static void writeAVROFile(File avroFile, Schema schema, GenericRecord genericRecord) throws IOException {

        List<GenericRecord> listGenericRecord = new ArrayList<GenericRecord>();
        listGenericRecord.add(genericRecord);

        writeAVROFile(avroFile, schema, listGenericRecord);

    }

    /**
     * Metodo que genera escribe un fichero AVRO con el contenido de listGenericRecord
     *
     * @param avroFile File con la referencia al fichero
     * @param schema Schema con el esquema AVRO
     * @param listGenericRecord List con la lista de objetos a escribir
     * @param codec CodecFactory con el codec de compresion a usar
     *
     */
    public static void writeAVROFile(File avroFile, Schema schema, List<GenericRecord> listGenericRecord, CodecFactory codec) throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.setCodec(codec);
        dataFileWriter.create(schema, avroFile);

        for (GenericRecord genericRecord : listGenericRecord) {
            dataFileWriter.append(genericRecord);
        }

        dataFileWriter.close();
    }

    /**
     * Metodo que genera escribe un fichero AVRO el contenido de genericRecord
     *
     * @param avroFile File con la referencia al fichero
     * @param schema Schema con el esquema AVRO
     * @param genericRecord GenericRecord con el objeto a serializar en fichero
     * @param codec CodecFactory con el codec de compresion a usar
     *
     */
    public static void writeAVROFile(File avroFile, Schema schema, GenericRecord genericRecord, CodecFactory codec) throws IOException {

        List<GenericRecord> listGenericRecord = new ArrayList<GenericRecord>();
        listGenericRecord.add(genericRecord);

        writeAVROFile(avroFile, schema, listGenericRecord, codec);
    }



    /**
     * Metodo que genera escribe un fichero AVRO con el contenido de listEnrichedEventBodyMapAvroString
     *
     * @param avroFile File con la referencia al fichero
     * @param listEnrichedEventBodyMapAvroString List  con la lista de objetos a escribir
     *
     */
    public static void writeAVROFile(File avroFile, List<EnrichedEventBodyMapAvroString> listEnrichedEventBodyMapAvroString) throws IOException {

        DatumWriter<EnrichedEventBodyMapAvroString> datumWriter = new SpecificDatumWriter<EnrichedEventBodyMapAvroString>(EnrichedEventBodyMapAvroString.class);
        DataFileWriter<EnrichedEventBodyMapAvroString> dataFileWriter = new DataFileWriter<EnrichedEventBodyMapAvroString>(datumWriter);

        EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroStringFirstElement = listEnrichedEventBodyMapAvroString.get(0);
        dataFileWriter.create(enrichedEventBodyMapAvroStringFirstElement.getSchema(), avroFile);

        for (EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString : listEnrichedEventBodyMapAvroString) {
            dataFileWriter.append(enrichedEventBodyMapAvroString);
        }
        dataFileWriter.close();

    }

    /**
     * Metodo que genera escribe un fichero AVRO con el contenido de enrichedEventBodyMapAvroString
     *
     * @param avroFile File con la referencia al fichero
     * @param enrichedEventBodyMapAvroString EnrichedEventBodyMapAvroString  con la lista de objetos a escribir
     *
     */
    public static void writeAVROFile(File avroFile,  EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString) throws IOException {

        List<EnrichedEventBodyMapAvroString> listEnrichedEventBodyMapAvroString = new ArrayList<EnrichedEventBodyMapAvroString>();
        listEnrichedEventBodyMapAvroString.add(enrichedEventBodyMapAvroString);

        writeAVROFile(avroFile, listEnrichedEventBodyMapAvroString);

    }


    public static <T> void writeReflectiveAVROFile(File avroFile, Schema schema, List<T> listObjects) throws IOException {

        DatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(datumWriter);

        T objectFirstElement = listObjects.get(0);
        dataFileWriter.create(schema, avroFile);

        for (T object : listObjects) {
            dataFileWriter.append(object);
        }
        dataFileWriter.close();

    }


    public static <T> void writeReflectiveAVROFile(File avroFile, Schema schema, T object) throws IOException {

        List<T> listObjects = new ArrayList<T>();
        listObjects.add(object);

        writeReflectiveAVROFile(avroFile, schema, listObjects);

    }

    /**
     * Metodo que genera un GenericRecord con el contenido del objeto enrichedEventBodyGeneric
     *
     * @param enrichedEventBodyGeneric EnrichedEventBodyGeneric que se convierte en GenericRecord
     * @param schema Schema con el schema utilizado en la conversion
     *
     * @return GenericRecord con el contenido de EnrichedEventBodyGeneric
     */
    public static GenericRecord enrichedEventBodyGeneric2GenericRecord(EnrichedEventBodyGeneric enrichedEventBodyGeneric, Schema schema) {

        GenericRecord datum = null;
        if (enrichedEventBodyGeneric.getExtraData() instanceof EnrichedEventBodyExtraData) {

            datum = new GenericData.Record(schema);
            GenericRecord datumExtraData = new GenericData.Record(schema.getField("extraData").schema());

            EnrichedEventBodyExtraData enrichedEventBodyExtraData = (EnrichedEventBodyExtraData) enrichedEventBodyGeneric.getExtraData();
            String message = enrichedEventBodyGeneric.getMessage();

            datum.put("message", message);

            datumExtraData.put("topic", enrichedEventBodyExtraData.getTopic());
            datumExtraData.put("timestamp", enrichedEventBodyExtraData.getTimestamp());
            datumExtraData.put("sha1Hex", enrichedEventBodyExtraData.getSha1Hex());
            datumExtraData.put("filePath", enrichedEventBodyExtraData.getFilePath());
            datumExtraData.put("fileName", enrichedEventBodyExtraData.getFileName());
            datumExtraData.put("lineNumber", enrichedEventBodyExtraData.getLineNumber());
            datumExtraData.put("type", enrichedEventBodyExtraData.getType());

            datum.put("extraData", datumExtraData);
        } else if (enrichedEventBodyGeneric.getExtraData() instanceof HashMap) {
            datum = new GenericData.Record(schema);

            Map<String, String> extraData = (Map<String, String>) enrichedEventBodyGeneric.getExtraData();
            String message = enrichedEventBodyGeneric.getMessage();

            datum.put("message", message);
            datum.put("extraData", extraData);

            return datum;
        }


        return datum;
    }




    /**
     * Metodo que genera un EnrichedEventBodyGeneric con el contenido del objeto GenericRecord
     *
     * @param genericRecord GenericRecord con el contenido a transformar en EnrichedEventBodyGeneric
     * @param isUTF8Class boolean indicando si la clase de los objetos que componen enericRecord es String o UTF-8
     * @param clazz Class indicando la clase de objeto que contiene el tipo generico
     *
     * @return EnrichedEventBodyGeneric con el contenido de GenericRecord
     */
    public static EnrichedEventBodyGeneric genericRecord2EnrichedEventBodyGeneric(GenericRecord genericRecord, boolean isUTF8Class, Class clazz) {

        EnrichedEventBodyGeneric enrichedEventBodyGeneric = null;

        if (clazz.equals(EnrichedEventBodyExtraData.class)) {
            if (!isUTF8Class) {
                String message = (String) genericRecord.get("message");

                GenericRecord genericRecordExtraData = (GenericRecord) genericRecord.get("extraData");
                EnrichedEventBodyExtraData enrichedEventBodyExtraData = new EnrichedEventBodyExtraData();

                String topic = genericRecordExtraData.get("topic") != null ? (String) genericRecordExtraData.get("topic") : null;
                String timestamp = genericRecordExtraData.get("timestamp") != null ? (String) genericRecordExtraData.get("timestamp") : null;
                String sha1Hex = genericRecordExtraData.get("sha1Hex") != null ? (String) genericRecordExtraData.get("sha1Hex") : null;
                String filePath = genericRecordExtraData.get("filePath") != null ? (String) genericRecordExtraData.get("filePath") : null;
                String fileName = genericRecordExtraData.get("fileName") != null ? (String) genericRecordExtraData.get("fileName") : null;
                String lineNumber = genericRecordExtraData.get("lineNumber") != null ? (String) genericRecordExtraData.get("lineNumber") : null;
                String type = genericRecordExtraData.get("type") != null ? (String) genericRecordExtraData.get("type") : null;

                enrichedEventBodyExtraData.setTopic(topic);
                enrichedEventBodyExtraData.setTimestamp(timestamp);
                enrichedEventBodyExtraData.setSha1Hex(sha1Hex);
                enrichedEventBodyExtraData.setFilePath(filePath);
                enrichedEventBodyExtraData.setFileName(fileName);
                enrichedEventBodyExtraData.setLineNumber(lineNumber);
                enrichedEventBodyExtraData.setTimestamp(timestamp);
                enrichedEventBodyExtraData.setType(type);

                enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(enrichedEventBodyExtraData, message, EnrichedEventBodyExtraData.class);
            } else {
                String message = genericRecord.get("message").toString();

                GenericRecord genericRecordExtraData = (GenericRecord) genericRecord.get("extraData");
                EnrichedEventBodyExtraData enrichedEventBodyExtraData = new EnrichedEventBodyExtraData();

                String topic = genericRecordExtraData.get("topic") != null ? genericRecordExtraData.get("topic").toString() : null;
                String timestamp = genericRecordExtraData.get("timestamp") != null ? genericRecordExtraData.get("timestamp").toString() : null;
                String sha1Hex = genericRecordExtraData.get("sha1Hex") != null ? genericRecordExtraData.get("sha1Hex").toString() : null;
                String filePath = genericRecordExtraData.get("filePath") != null ? genericRecordExtraData.get("filePath").toString() : null;
                String fileName = genericRecordExtraData.get("fileName") != null ? genericRecordExtraData.get("fileName").toString() : null;
                String lineNumber = genericRecordExtraData.get("lineNumber") != null ? genericRecordExtraData.get("lineNumber").toString() : null;
                String type = genericRecordExtraData.get("type") != null ? genericRecordExtraData.get("type").toString() : null;

                enrichedEventBodyExtraData.setTopic(topic);
                enrichedEventBodyExtraData.setTimestamp(timestamp);
                enrichedEventBodyExtraData.setSha1Hex(sha1Hex);
                enrichedEventBodyExtraData.setFilePath(filePath);
                enrichedEventBodyExtraData.setFileName(fileName);
                enrichedEventBodyExtraData.setLineNumber(lineNumber);
                enrichedEventBodyExtraData.setTimestamp(timestamp);
                enrichedEventBodyExtraData.setType(type);

                enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(enrichedEventBodyExtraData, message, EnrichedEventBodyExtraData.class);

            }
        } else if (clazz.equals(HashMap.class)) {

            if (!isUTF8Class) {
                String message = (String) genericRecord.get("message");
                HashMap extraData = (HashMap)  genericRecord.get("extraData");
                Class theClazz = extraData.getClass();

                enrichedEventBodyGeneric = new EnrichedEventBodyGeneric(extraData, message, theClazz);
            } else {
                String message = genericRecord.get("message").toString();
                HashMap extraData = (HashMap)  genericRecord.get("extraData");
                Class theClazz = extraData.getClass();

                //Convertimos el map (Map<UTF-8, UTF-8>) en Map<String, String>
                enrichedEventBodyGeneric = new EnrichedEventBodyGeneric(null, message, theClazz);
                Map<String,String> mapExtraDataString = (HashMap) enrichedEventBodyGeneric.getExtraData();

                Set<Map.Entry> setEntry = extraData.entrySet();

                for (Map.Entry entry : setEntry) {
                    String keyString = entry.getKey().toString();
                    String valueString = entry.getValue().toString();

                    mapExtraDataString.put(keyString, valueString);
                }

            }

        }

        return enrichedEventBodyGeneric;

    }

    public static <T> EnrichedEventBodyGeneric<T> specificAvroClass2EnrichedEventBodyGeneric(Object specificAvroClassObject) {

        EnrichedEventBodyGeneric enrichedEventBodyGeneric = null;

        if (specificAvroClassObject instanceof EnrichedEventBodyGenericAvroString) {

            EnrichedEventBodyGenericAvroString enrichedEventBodyGenericAvroString = (EnrichedEventBodyGenericAvroString) specificAvroClassObject;

            EnrichedEventBodyExtraData enrichedEventBodyExtraData = new EnrichedEventBodyExtraData();
            EnrichedEventBodyExtraDataAvroString enrichedEventBodyExtraDataAvroString = enrichedEventBodyGenericAvroString.getExtraData();

            enrichedEventBodyExtraData.setTopic(enrichedEventBodyExtraDataAvroString.getTopic());
            enrichedEventBodyExtraData.setTimestamp(enrichedEventBodyExtraDataAvroString.getTimestamp());
            enrichedEventBodyExtraData.setSha1Hex(enrichedEventBodyExtraDataAvroString.getSha1Hex());
            enrichedEventBodyExtraData.setFilePath(enrichedEventBodyExtraDataAvroString.getFilePath());
            enrichedEventBodyExtraData.setFileName(enrichedEventBodyExtraDataAvroString.getFileName());
            enrichedEventBodyExtraData.setLineNumber(enrichedEventBodyExtraDataAvroString.getLineNumber());
            enrichedEventBodyExtraData.setType(enrichedEventBodyExtraDataAvroString.getType());

            enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<EnrichedEventBodyExtraData>(enrichedEventBodyExtraData, enrichedEventBodyGenericAvroString.getMessage(), EnrichedEventBodyExtraData.class);


        } else if (specificAvroClassObject instanceof EnrichedEventBodyMapAvroString) {

            EnrichedEventBodyMapAvroString enrichedEventBodyMapAvroString = (EnrichedEventBodyMapAvroString) specificAvroClassObject;

            HashMap<String, String> mapExtraDataMapAvroString = (HashMap<String, String>) enrichedEventBodyMapAvroString.getExtraData();
            Class clazz = mapExtraDataMapAvroString.getClass();

            enrichedEventBodyGeneric = new EnrichedEventBodyGeneric<HashMap<String, String>>(mapExtraDataMapAvroString, enrichedEventBodyMapAvroString.getMessage(), clazz);

        }

        return enrichedEventBodyGeneric;
    }


    public static <T> Object enrichedEventBodyGeneric2SpecificAvroClass(EnrichedEventBodyGeneric<T> enrichedEventBodyGeneric) {

        Object specificAvroClass = null;

        T extraData = enrichedEventBodyGeneric.getExtraData();

        if (extraData instanceof EnrichedEventBodyExtraData) {

            EnrichedEventBodyExtraDataAvroString enrichedEventBodyExtraDataAvroString = new EnrichedEventBodyExtraDataAvroString();
            EnrichedEventBodyExtraData enrichedEventBodyExtraData = (EnrichedEventBodyExtraData) extraData;

            enrichedEventBodyExtraDataAvroString.setTopic(enrichedEventBodyExtraData.getTopic());
            enrichedEventBodyExtraDataAvroString.setTimestamp(enrichedEventBodyExtraData.getTimestamp());
            enrichedEventBodyExtraDataAvroString.setSha1Hex(enrichedEventBodyExtraData.getSha1Hex());
            enrichedEventBodyExtraDataAvroString.setFilePath(enrichedEventBodyExtraData.getFilePath());
            enrichedEventBodyExtraDataAvroString.setFileName(enrichedEventBodyExtraData.getFileName());
            enrichedEventBodyExtraDataAvroString.setLineNumber(enrichedEventBodyExtraData.getLineNumber());
            enrichedEventBodyExtraDataAvroString.setType(enrichedEventBodyExtraData.getType());

            specificAvroClass = new EnrichedEventBodyGenericAvroString(enrichedEventBodyExtraDataAvroString, enrichedEventBodyGeneric.getMessage());

        } else if (extraData instanceof HashMap) {

            HashMap<String, String> mapEnrichedEventBodyExtraData = (HashMap<String, String>) extraData;
            specificAvroClass = new EnrichedEventBodyMapAvroString(mapEnrichedEventBodyExtraData, enrichedEventBodyGeneric.getMessage());

        }

        return specificAvroClass;
    }

}
