package com.github.minyk.hive.serde;

import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.Text;

public class TestGrokSerDe extends TestCase {

    private AbstractSerDe createSerDe(String fieldNames, String fieldTypes,
                                      String inputPattern, String outputFormatString) throws Throwable {
        Properties schema = new Properties();
        schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
        schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);
        schema.setProperty("input.pattern", inputPattern);
        schema.setProperty("output.format.string", outputFormatString);

        GrokSerDe serde = new GrokSerDe();
        SerDeUtils.initializeSerDe(serde, new Configuration(), schema, null);
        return serde;
    }

    /**
     * Test the GrokSerDe class.
     */
    public void testGrokSerDe() throws Throwable {
        try {
            // Create the SerDe
            AbstractSerDe serDe = createSerDe(
                    "clientip,ident,auth,timestamp,request,response,bytes,referrer,agent",
                    "string,string,string,string,string,string,string,string,string",
                    "%{COMBINEDAPACHELOG}",
                    "%1$s %2$s %3$s [%4$s] \"GET %5$s HTTP/1.1\" %6$s %7$s \"%8$s\" \"%9$s\"");

            // Data
            Text t = new Text(
                    "127.0.0.1 - - [26/May/2009:00:00:00 +0000] "
                            + "\"GET /someurl/?track=Blabla(Main) HTTP/1.1\" 200 5864 \"-\" "
                            + "\"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) "
                            + "AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.65 Safari/525.19\"");

            // Deserialize
            Object row = serDe.deserialize(t);
            ObjectInspector rowOI = serDe.getObjectInspector();

            System.out.println("Deserialized row: " + row);

            // Serialize
            Text serialized = (Text) serDe.serialize(row, rowOI);
            assertEquals(t, serialized);

            // Do some changes (optional)
            ObjectInspector standardWritableRowOI = ObjectInspectorUtils
                    .getStandardObjectInspector(rowOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
            Object standardWritableRow = ObjectInspectorUtils.copyToStandardObject(
                    row, rowOI, ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);

            // Serialize
            serialized = (Text) serDe.serialize(standardWritableRow,
                    standardWritableRowOI);
            System.out.println("Serialized row: " + serialized);
            assertEquals(t, serialized);

        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }
}
