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
    
    public void testMatchFilesRegexp(){
        
        System.out.println("matchFilesRegexp");
        
    }
    
}
