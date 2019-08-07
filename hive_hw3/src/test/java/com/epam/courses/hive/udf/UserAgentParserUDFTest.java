package com.epam.courses.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import java.io.IOException;
import java.util.Map;

import static com.epam.courses.hive.udf.UserAgentParserUDF.KEY_BROWSER;
import static com.epam.courses.hive.udf.UserAgentParserUDF.KEY_DEVICE;
import static com.epam.courses.hive.udf.UserAgentParserUDF.KEY_OS;
import static com.epam.courses.hive.udf.UserAgentParserUDF.KEY_USER_AGENT;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;



public class UserAgentParserUDFTest
{

    @Test
    public void testFireFox() throws IOException  {
        final String testString = "Mozilla/5.0 (Windows NT 5.1; rv:24.0) Gecko/20100101 Firefox/24.0";

        UserAgentParserUDF userAgentParserUDF = new UserAgentParserUDF();
        Map<String, String> map = userAgentParserUDF.evaluate(new Text(testString));
        assertEquals("Windows XP", map.get(KEY_OS));
        assertEquals("FIREFOX", map.get(KEY_BROWSER));
        assertEquals("Computer", map.get(KEY_DEVICE));
        assertEquals("Browser", map.get(KEY_USER_AGENT));
    }

    @Test
    public void testChrome() throws IOException  {
        final String testString = "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1";

        UserAgentParserUDF userAgentParserUDF = new UserAgentParserUDF();
        Map<String, String> map = userAgentParserUDF.evaluate(new Text(testString));
        assertEquals("Windows XP", map.get(KEY_OS));
        assertEquals("CHROME", map.get(KEY_BROWSER));
        assertEquals("Computer", map.get(KEY_DEVICE));
        assertEquals("Browser", map.get(KEY_USER_AGENT));
    }

    @Test
    public void testIE() throws IOException  {
        final String testString = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.4; .NET CLR 2.0.50727; .NET CLR 1.1.4322; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)";

        UserAgentParserUDF userAgentParserUDF = new UserAgentParserUDF();
        Map<String, String> map = userAgentParserUDF.evaluate(new Text(testString));
        assertEquals("Windows XP", map.get(KEY_OS));
        assertEquals("IE", map.get(KEY_BROWSER));
        assertEquals("Computer", map.get(KEY_DEVICE));
        assertEquals("Browser", map.get(KEY_USER_AGENT));
    }

    @Test
    public void testAndroid() throws IOException  {
        final String testString = "CoolPad8020+_CMCC_TD/1.08 Linux/2.6.35 Android/2.3.5 Release/03.29.2012 Mozilla/5.0 AppleWebKit/533.1 Version/4.0 Mobile Safari/533.1";

            UserAgentParserUDF userAgentParserUDF = new UserAgentParserUDF();
        Map<String, String> map = userAgentParserUDF.evaluate(new Text(testString));
        assertEquals("Android", map.get(KEY_OS));
        assertEquals("SAFARI", map.get(KEY_BROWSER));
        assertEquals("Mobile", map.get(KEY_DEVICE));
        assertEquals("Browser (mobile)", map.get(KEY_USER_AGENT));
    }

    @Test
    public void testEmptyString() throws IOException  {
        UserAgentParserUDF userAgentParserUDF = new UserAgentParserUDF();
        Map<String, String> map = userAgentParserUDF.evaluate(new Text());
        assertNotNull(map);
        assertEquals(4, map.size());
        assertEquals("Unknown", map.get(KEY_OS));
        assertEquals("Unknown", map.get(KEY_OS));
        assertEquals("Unknown", map.get(KEY_DEVICE));
        assertEquals("unknown", map.get(KEY_USER_AGENT));
    }
}
