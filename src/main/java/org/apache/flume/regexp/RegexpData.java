package org.apache.flume.regexp;

import java.util.Map;
import java.util.HashMap;



import org.apache.flume.Context;

import com.google.code.regexp.Pattern;
import com.google.code.regexp.Matcher;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Luis Lázaro lalazaro@keedio.com Keedio
 */
public class RegexpData {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RegexpData.class);

    private Map<String, Pattern> regexpMap = new HashMap<>();
    private Map<String, String> matchesMap = new HashMap<>();

    private final String CUSTOM_REGEXPS = "properties.regexp.";

    public RegexpData(Context context) {
        Map<String, String> subProperties = context.getSubProperties(CUSTOM_REGEXPS);

        for (Map.Entry<String, String> entry : subProperties.entrySet()) {
            regexpMap.put(entry.getKey(), Pattern.compile(entry.getValue()));
        }

    }

    /**
     * Aplicar regexps al mensaje, de todas las regexps enriquequecemos usando
     * la primera regexp que nos devuelva algun resultado.
     *
     * @param message
     * @return map
     */
    public Map<String, String> applyRegexps(String message) {

        for (Map.Entry<String, Pattern> entry : regexpMap.entrySet()) {
            Matcher m = entry.getValue().matcher(message); 
            if (m.find()){
                matchesMap.putAll(m.namedGroups());
                break;
            }
        }
        return matchesMap;
    }

}//endofclass
