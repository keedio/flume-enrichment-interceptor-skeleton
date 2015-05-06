package org.apache.flume.regexp;

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
 * @author Luis Lázaro lalazaro@keedio.com Keedio
 */
public class RegexpData {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RegexpData.class);
    
    private Map<String, String> regexpMap = new HashMap<>();
    private Map<String, String> matchesMap = new HashMap<>();

    private final String CUSTOM_REGEXPS = "properties.regexp.";
    private final String FOLDER_LOGS = "properties.folder.logs";

    public RegexpData(Context context) {
        regexpMap = context.getSubProperties(CUSTOM_REGEXPS);
        try {
            matchesMap = matchFilesRegexp();
        } catch (IOException e) {
            logger.error("", e);
        }

    }

    /**
     * Retrieve files from specified folder, for each found file, a list of its
     * lines will be created, for each list created, a regexp will be matched.
     * 
     * @return a map of matches as HashMap<(name-group-capture), match> 
     * @throws IOException
     */
    public Map<String, String> matchFilesRegexp() throws IOException {
        Path start = Paths.get(FOLDER_LOGS);

        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                List<String> linesfile = Files.readAllLines(file, Charset.defaultCharset());

                for (String line : linesfile) {
                    for (String regexp : regexpMap.values()) {
                        Matcher m = Pattern.compile(regexp).matcher(line);
                        matchesMap.putAll(m.namedGroups());
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }

        });
        return matchesMap;

    }

    /**
     * @return the matchesMap
     */
    public Map<String, String> getMatchesMap() {
        return matchesMap;
    }

}//endofclass
