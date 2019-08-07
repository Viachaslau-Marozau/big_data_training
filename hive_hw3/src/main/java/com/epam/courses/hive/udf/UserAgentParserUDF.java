package com.epam.courses.hive.udf;

import nl.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Tasks:
 * - Write custom UDF which can parse any user agent (UA) string into separate fields
 * - Use data from you UDF and find most popular device, browser, OS for each city.
 * <p>
 * UDF input example:
 * "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
 * <p>
 * UDF Output example:
 * device  : Personal computer
 * OS name : Windows 7
 * Browser : IE
 * UA      : Browser
 */
@Description(
    name = "UserAgentParserUDF",
    value = "_FUNC_(string) - parse any user agent (UA) string into separate fields: device, os, browser, ua",
    extended = "Example:\n" +
        "  > select cityid, ua_mapper(agent) from weblog"
)
public final class UserAgentParserUDF extends UDF
{
    /**
     * Device key
     */
    public static final String KEY_DEVICE = "device";
    /**
     * OS name key
     */
    public static final String KEY_OS = "os";
    /**
     * Browser key
     */
    public static final String KEY_BROWSER = "browser";
    /**
     * UA key
     */
    public static final String KEY_USER_AGENT = "ua";

    /**
     * Parse any user agent (UA) string into separate fields
     *
     * @param s - input text
     * @return parse any user agent (UA) string into separate fields
     */
    public Map<String, String> evaluate(final Text s)
    {
        UserAgent userAgent = UserAgent.parseUserAgentString(s.toString());
        Map<String, String> map = new HashMap<String, String>();
        map.put(KEY_DEVICE, userAgent.getOperatingSystem().getDeviceType().getName());
        map.put(KEY_OS, userAgent.getOperatingSystem().getName());
        map.put(KEY_BROWSER, userAgent.getBrowser().getGroup().toString());
        map.put(KEY_USER_AGENT, userAgent.getBrowser().getBrowserType().getName());
        return map;
    }
}