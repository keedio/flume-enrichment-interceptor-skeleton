/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.keedio.flume.interceptor.enrichment.regexp;

import com.google.code.regexp.Matcher;
import com.google.code.regexp.Pattern;
import junit.framework.TestCase;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichmentInterceptor;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author luislazaro
 */
public class RegexpDataTest extends TestCase {

    public RegexpDataTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test of getMatchesMap method, of class RegexpData.
     */
//    public void testGetMatchesMap() {
//        System.out.println("getMatchesMap");
//        RegexpData instance = null;
//        Map<String, String> expResult = null;
//        Map<String, String> result = instance.getMatchesMap();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
    /**
     * Test of matchFilesRegexp(), of class RegexpData
     */
    public void testMatchFiles_SINGLE_Regexp() {
        System.out.println("matchFiles_single_Regexp");

        Path file = Paths.get("src/test/resources/file.log");

        Map<String, String> regexpMap = new HashMap<>();
        Map<String, String> matchesMap = new HashMap<>();

        regexpMap.put("1", "(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s"
                + "(?<time>\\d{2}:\\d{2}:\\d{2}+)\\s"
                + "(?<time-taken>\\d{1}+)\\s");

        try {
            List<String> linesfile = Files.readAllLines(file, Charset.defaultCharset());
            for (String line : linesfile) {
                for (String regexp : regexpMap.values()) {
                    Matcher m = Pattern.compile(regexp).matcher(line);
                    matchesMap.putAll(m.namedGroups());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(matchesMap);

    }

    public void testMatchFiles_SINGLE_Regexp_SEVERAL_lines() {
        System.out.println("MatchFiles_SINGLE_Regexp_SEVERAL_lines_Single_file");

        Path file = Paths.get("src/test/resources/file3.log");

        Map<String, String> regexpMap = new HashMap<>();
        Map<String, String> matchesMap = new HashMap<>();

        regexpMap.put("1", "(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s"
                + "(?<time>\\d{2}:\\d{2}:\\d{2}+)\\s"
                + "(?<time-taken>\\d{1}+)\\s");

        try {
            List<String> linesfile = Files.readAllLines(file, Charset.defaultCharset());
            for (String line : linesfile) {
                for (String regexp : regexpMap.values()) {
                    Matcher m = Pattern.compile(regexp).matcher(line);
                    matchesMap.putAll(m.namedGroups());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(matchesMap);

    }

    public void testMatchFiles_several_Regexp() {
        System.out.println("matchFiles_several_Regexp");

        Path file = Paths.get("src/test/resources/file.log");

        Map<String, String> regexpMap = new HashMap<>();
        Map<String, String> matchesMap = new HashMap<>();

        regexpMap.put("1", "(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s");
        regexpMap.put("2", "(?<time>\\d{2}:\\d{2}:\\d{2}+)\\s");

        try {
            List<String> linesfile = Files.readAllLines(file, Charset.defaultCharset());
            for (String line : linesfile) {
                for (String regexp : regexpMap.values()) {
                    Matcher m = Pattern.compile(regexp).matcher(line);
                    matchesMap.putAll(m.namedGroups());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(matchesMap);

    }

    @Test
    public void testSimpleNamedGroup() {
        try {
            Event event = createEvent("2015-04-23 07:16:08 1 85.31.12.49");
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put("custom.regexp.1","(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s");
            EnrichmentInterceptor interceptor = createEnrichedInterceptor(event, context);
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getExtraData().containsKey("date"));
            assertEquals(enrichedEventBody.getExtraData().get("date"),"2015-04-23");
        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testNoNamedGroupsDefaultKeyValue() {
        try {
            Event event = createEvent("time=20:11:10 devname=FGT3HD3915807828 devid=FGT3HD3915808013 some=\"quoted content\"");
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put("custom.regexp.1","(\\w+)=([^\"\\s]+)");  // For non-quoted key-value pairs
            context.put("custom.regexp.2","(\\w+)=[\"]([^\"]+)[\"]");  // For quoted key-value pairs
            EnrichmentInterceptor interceptor = createEnrichedInterceptor(event, context);
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getExtraData().containsKey("time"));
            assertEquals(enrichedEventBody.getExtraData().get("time"),"20:11:10");
            assertTrue(enrichedEventBody.getExtraData().containsKey("devname"));
            assertEquals(enrichedEventBody.getExtraData().get("devname"),"FGT3HD3915807828");
            assertTrue(enrichedEventBody.getExtraData().containsKey("devid"));
            assertEquals(enrichedEventBody.getExtraData().get("devid"),"FGT3HD3915808013");
            assertTrue(enrichedEventBody.getExtraData().containsKey("some"));
            assertEquals(enrichedEventBody.getExtraData().get("some"),"quoted content");
        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testNoNamedGroupsDifferentKeyValueGroups() {
        try {
            Event event = createEvent("time=20:11:10 devname=FGT3HD3915807828 devid=FGT3HD3915808013 some=\"quoted content\"");
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put("custom.regexp.1","(\\w+)(=)([^\"\\s]+)");  // For non-quoted key-value pairs
            context.put("custom.regexp.2","(\\w+)(=)[\"]([^\"]+)[\"]");  // For quoted key-value pairs
            //context.put("custom.group.regexp.key", "1");  // Default: 1
            context.put("custom.group.regexp.value", "3");  // Default: 2
            EnrichmentInterceptor interceptor = createEnrichedInterceptor(event, context);
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getExtraData().containsKey("time"));
            assertEquals(enrichedEventBody.getExtraData().get("time"),"20:11:10");
            assertTrue(enrichedEventBody.getExtraData().containsKey("devname"));
            assertEquals(enrichedEventBody.getExtraData().get("devname"),"FGT3HD3915807828");
            assertTrue(enrichedEventBody.getExtraData().containsKey("devid"));
            assertEquals(enrichedEventBody.getExtraData().get("devid"),"FGT3HD3915808013");
            assertTrue(enrichedEventBody.getExtraData().containsKey("some"));
            assertEquals(enrichedEventBody.getExtraData().get("some"),"quoted content");
        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }

    @Test
    public void testEmptyGroups() {
        try {
            Event event = createEvent("");
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put("custom.regexp.1","(\\w+)=([^\"\\s]+)");  // For non-quoted key-value pairs
            context.put("custom.regexp.2","(\\w+)=[\"]([^\"]+)[\"]");  // For quoted key-value pairs
            EnrichmentInterceptor interceptor = createEnrichedInterceptor(event, context);
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertEquals(enrichedEventBody.getExtraData().size(), 0);
        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }



    @Test
    public void testNoNamedGroupsUrlEncoded() {
        try {
            Event event = createEvent("time=20:11:10 devname=FGT3HD3915807828 devid=FGT3HD3915808013 some=\"quoted content\"");
            Context context = new Context();
            context.put(EnrichmentInterceptor.EVENT_TYPE, "DEFAULT");
            context.put("custom.regexp.1","%28%5Cw%2B%29%3D%28%5B%5E%5C%22%5Cs%5D%2B%29");  // For non-quoted key-value pairs
            context.put("custom.regexp.2","%28%5Cw%2B%29%3D%5B%5C%22%5D%28%5B%5E%5C%22%5D%2B%29%5B%5C%22%5D");  // For quoted key-value pairs
            context.put("regexp.is.urlencoded", "true");
            EnrichmentInterceptor interceptor = createEnrichedInterceptor(event, context);
            Event intercepted = interceptor.intercept(event);

            EnrichedEventBody enrichedEventBody = EnrichedEventBody.createFromEventBody(intercepted.getBody(), true);

            assertTrue(enrichedEventBody.getExtraData().containsKey("time"));
            assertEquals(enrichedEventBody.getExtraData().get("time"),"20:11:10");
            assertTrue(enrichedEventBody.getExtraData().containsKey("devname"));
            assertEquals(enrichedEventBody.getExtraData().get("devname"),"FGT3HD3915807828");
            assertTrue(enrichedEventBody.getExtraData().containsKey("devid"));
            assertEquals(enrichedEventBody.getExtraData().get("devid"),"FGT3HD3915808013");
            assertTrue(enrichedEventBody.getExtraData().containsKey("some"));
            assertEquals(enrichedEventBody.getExtraData().get("some"),"quoted content");
        } catch (IOException e) {
            e.printStackTrace();
            junit.framework.Assert.fail();
        }
    }


    protected Event createEvent(String message) {
        HashMap headers = new HashMap();
        headers.put("h1", "value1");
        return EventBuilder.withBody(message.getBytes(), headers);
    }


    protected EnrichmentInterceptor createEnrichedInterceptor(Event event, Context context) {


        EnrichmentInterceptor.Builder builder = new EnrichmentInterceptor.EnrichmentBuilder();
        builder.configure(context);
        EnrichmentInterceptor interceptor = (EnrichmentInterceptor) builder.build();
        interceptor.initialize();
        return interceptor;
    }

//    public void testMatchFiles_several_Regexp_and_files(){
//         System.out.println("matchFiles_several_Regexp_and_files");
//         
//         Path file = Paths.get("srct/test/resources/file.log");
//         Path file2 = Paths.get("srct/test/resources/file2.log");
//         
//         
//         Map<String,String> regexpMap = new HashMap<>();
//         Map<String,String> matchesMap = new HashMap<>();
//         
//         regexpMap.put("1","(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s");
//         regexpMap.put("2","(?<time>\\d{2}:\\d{2}:\\d{2}+)\\s");
//         
//         
//         
//         
//         try {
//          List<String> linesfile = Files.readAllLines(file, Charset.defaultCharset());
//           for (String line : linesfile) {
//                    for (String regexp : regexpMap.values()) {
//                        Matcher m = Pattern.compile(regexp).matcher(line);
//                        matchesMap.putAll(m.namedGroups());
//                        System.out.println(matchesMap);
//                    }
//                }
//         } catch(IOException e){
//             e.printStackTrace();
//         }
//         
//         
//         try {
//          List<String> linesfile = Files.readAllLines(file2, Charset.defaultCharset());
//           for (String line : linesfile) {
//                    for (String regexp : regexpMap.values()) {
//                        Matcher m = Pattern.compile(regexp).matcher(line);
//                        matchesMap.putAll(m.namedGroups());
//                        System.out.println(matchesMap);
//                    }
//                }
//         } catch(IOException e){
//             e.printStackTrace();
//         }
//         
//         //System.out.println(matchesMap);
//        
//    }
   
}//endofclass
