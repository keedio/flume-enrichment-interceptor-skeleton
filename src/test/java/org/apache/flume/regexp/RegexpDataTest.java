/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.flume.regexp;

import java.util.Map;
import junit.framework.TestCase;

import java.util.Map;
import java.util.List;
import java.util.HashMap;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;

import org.apache.flume.Context;

import com.google.code.regexp.Pattern;
import com.google.code.regexp.Matcher;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

import com.google.common.collect.Maps;

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
    
    public void testMatchFiles_SINGLE_Regexp(){
         System.out.println("matchFiles_single_Regexp");
         
         Path file = Paths.get("/var/tmp/file.log");
         
         
         Map<String,String> regexpMap = new HashMap<>();
         Map<String,String> matchesMap = new HashMap<>();
         
         regexpMap.put("1","(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s"
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
         } catch(IOException e){
             e.printStackTrace();
         }
         
         System.out.println(matchesMap);
        
    }
    
    public void testMatchFiles_SINGLE_Regexp_SEVERAL_lines(){
         System.out.println("MatchFiles_SINGLE_Regexp_SEVERAL_lines_Single_file");
         
         Path file = Paths.get("/var/tmp/file3.log");
         
         
         Map<String,String> regexpMap = new HashMap<>();
         Map<String,String> matchesMap = new HashMap<>();
         
         regexpMap.put("1","(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s"
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
         } catch(IOException e){
             e.printStackTrace();
         }
         
         System.out.println(matchesMap);
        
    }
    
    public void testMatchFiles_several_Regexp(){
         System.out.println("matchFiles_several_Regexp");
         
         Path file = Paths.get("/var/tmp/file.log");
         
         
         Map<String,String> regexpMap = new HashMap<>();
         Map<String,String> matchesMap = new HashMap<>();
         
         regexpMap.put("1","(?<date>\\d{4}-\\d{2}-\\d{2}+)\\s");
         regexpMap.put("2","(?<time>\\d{2}:\\d{2}:\\d{2}+)\\s");
        
         
         
         
         try {
          List<String> linesfile = Files.readAllLines(file, Charset.defaultCharset());
           for (String line : linesfile) {
                    for (String regexp : regexpMap.values()) {
                        Matcher m = Pattern.compile(regexp).matcher(line);
                        matchesMap.putAll(m.namedGroups());
                    }
                }
         } catch(IOException e){
             e.printStackTrace();
         }
         
         System.out.println(matchesMap);
        
    }
    
    
    
//    public void testMatchFiles_several_Regexp_and_files(){
//         System.out.println("matchFiles_several_Regexp_and_files");
//         
//         Path file = Paths.get("/var/tmp/file.log");
//         Path file2 = Paths.get("/var/tmp/file2.log");
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
    
}
