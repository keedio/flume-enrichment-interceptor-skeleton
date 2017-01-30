package org.keedio.flume.interceptor.enrichment.regexp;

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

    private final String CUSTOM_REGEXPS = "custom.regexp.";

    private Integer regexpKey;
    private Integer regexpValue;

    public RegexpData(Context context) {
        Map<String, String> subProperties = context.getSubProperties(CUSTOM_REGEXPS);

        for (Map.Entry<String, String> entry : subProperties.entrySet()) {
            regexpMap.put(entry.getKey(), Pattern.compile(entry.getValue()));
        }

        this.regexpKey = context.getInteger("custom.group.regexp.key", 1);
        this.regexpValue = context.getInteger("custom.group.regexp.value", 2);
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

            if (m.namedGroups().size() > 0) {
                matchesMap.putAll(m.namedGroups());
                break;
            } else {
                // First match is already catched by previous namedGroups() call, so we reset it
                m.reset();
                // Useful for regexps with <key, value> pairs
                while (m.find() && m.groupCount() > 0){
                     matchesMap.put(m.group(this.regexpKey), m.group(this.regexpValue));
                }
            }
        }
        return matchesMap;
    }

}//endofclass
