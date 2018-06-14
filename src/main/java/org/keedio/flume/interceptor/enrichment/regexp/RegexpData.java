package org.keedio.flume.interceptor.enrichment.regexp;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.flume.Context;

import com.google.code.regexp.Pattern;
import com.google.code.regexp.Matcher;
import org.slf4j.LoggerFactory;

/**
 * @author Luis Lázaro lalazaro@keedio.com Keedio
 */
public class RegexpData {

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RegexpData.class);

  private Map<String, Pattern> regexpMap = new HashMap<>();
  private ConcurrentMap<String, String> matchesMap = new ConcurrentHashMap<>();

  private final String CUSTOM_REGEXPS = "custom.regexp.";

  private boolean isUrlEncoded;

  private Integer regexpKey;
  private Integer regexpValue;

  public RegexpData(Context context) {
    Map<String, String> subProperties = context.getSubProperties(CUSTOM_REGEXPS);

    this.isUrlEncoded = context.getBoolean("regexp.is.urlencoded", false);

    for (Map.Entry<String, String> entry : subProperties.entrySet()) {
      try {
        regexpMap.put(entry.getKey(), Pattern.compile(
          (this.isUrlEncoded) ?
            URLDecoder.decode(entry.getValue(), "UTF-8") :
            entry.getValue()));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
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
  public ConcurrentMap<String, String> applyRegexps(String message) {
    logger.debug("Message: " + message);
    matchesMap.clear();
    for (Map.Entry<String, Pattern> entry : regexpMap.entrySet()) {
      logger.debug("Regexp: " + entry.getKey() + " |||| " + entry.getValue());
      Matcher m = entry.getValue().matcher(message);

      if (m.namedGroups().size() > 0) {
        logger.debug("WITH NAMED GROUPS");
        for (Map.Entry<String, String> group : m.namedGroups().entrySet()) {
          matchesMap.put(group.getKey(), group.getValue());
        }
      } else {
        logger.debug("WITHOUT NAMED GROUPS");
        // First match is already catched by previous namedGroups() call, so we reset it
        m.reset();
        // Useful for regexps with <key, value> pairs
        while (m.find() && m.groupCount() > 0) {
          logger.debug("Group 0: " + m.group(0));
          logger.debug("Group 1: " + m.group(this.regexpKey));
          logger.debug("Group 2: " + m.group(this.regexpValue));
          matchesMap.put(m.group(this.regexpKey), m.group(this.regexpValue));
        }
      }
    }

    return matchesMap;
  }

}//endofclass
