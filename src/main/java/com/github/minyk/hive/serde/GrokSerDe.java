package com.github.minyk.hive.serde;

import com.google.common.io.Resources;
import io.krakens.grok.api.Grok;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.codehaus.jackson.map.ObjectMapper;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class GrokSerDe extends AbstractSerDe {

    private Logger LOG = LoggerFactory.getLogger(GrokSerDe.class);

    public static final String INPUT_PATTERN = "input.pattern";
    public static final String OUTPUT_FORMAT_STRING = "output.format.string";

    int numColumns;
    String inputPattern;
    String outputFormatString;

//    private List<String> columnNames;
    StructTypeInfo rowTypeInfo;
    ObjectInspector rowOI;
    ArrayList<String> row;

    GrokCompiler compiler;

    public void initialize(Configuration configuration, Properties properties) throws SerDeException {

        inputPattern = properties.getProperty(INPUT_PATTERN);
        outputFormatString = properties.getProperty(OUTPUT_FORMAT_STRING);

        String colNamesStr = properties.getProperty(serdeConstants.LIST_COLUMNS);
        final String columnNameDelimiter = properties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? properties
                .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
        List<String> columnNames = Arrays.asList(colNamesStr.split(columnNameDelimiter));

        String colTypesStr = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();

        // StandardStruct uses ArrayList to store the row.
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

        // Constructing the row object, etc, which will be reused for all rows.
        row = new ArrayList<String>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }
        outputFields = new Object[numColumns];
        outputRowText = new Text();

        // init grok
        compiler = GrokCompiler.newInstance();
        compiler.registerDefaultPatterns();
//        compiler.register(Resources.getResource("sample-grok.pattern").openStream());

        LOG.debug("SerDe initialized");
    }

    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    Object[] outputFields;
    Text outputRowText;

    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        if (outputFormatString == null) {
            throw new SerDeException(
                    "Cannot write data into table because \"output.format.string\""
                            + " is not specified in serde properties of the table.");
        }

        // Get all the fields out.
        // NOTE: The correct way to get fields out of the row is to use
        // objInspector.
        // The obj can be a Java ArrayList, or a Java class, or a byte[] or
        // whatever.
        // The only way to access the data inside the obj is through
        // ObjectInspector.

        StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
        List<? extends StructField> outputFieldRefs = outputRowOI
                .getAllStructFieldRefs();
        if (outputFieldRefs.size() != numColumns) {
            throw new SerDeException("Cannot serialize the object because there are "
                    + outputFieldRefs.size() + " fields but the table has " + numColumns
                    + " columns.");
        }

        // Get all data out.
        for (int c = 0; c < numColumns; c++) {
            Object field = outputRowOI
                    .getStructFieldData(obj, outputFieldRefs.get(c));
            ObjectInspector fieldOI = outputFieldRefs.get(c)
                    .getFieldObjectInspector();
            // The data must be of type String
            StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
            // Convert the field to Java class String, because objects of String type
            // can be
            // stored in String, Text, or some other classes.
            outputFields[c] = fieldStringOI.getPrimitiveJavaObject(field);
        }

        // Format the String
        String outputRowString = null;
        try {
            outputRowString = String.format(outputFormatString, outputFields);
        } catch (MissingFormatArgumentException e) {
            throw new SerDeException("The table contains " + numColumns
                    + " columns, but the outputFormatString is asking for more.", e);
        }
        outputRowText.set(outputRowString);
        return outputRowText;
    }

    public Object deserialize(Writable blob) throws SerDeException {
        row.clear();
        try {
            parseMessage(blob.toString());
        } catch (ClassCastException ex) {
            ex.printStackTrace();
            throw new SerDeException(ex);
        }

        return row;
    }

    public void parseMessage(String msg) throws ClassCastException {
        LOG.debug("parse: {} with {}", msg, inputPattern);

        Grok grok = compiler.compile(inputPattern);
        Match match = grok.match(msg);
        Map<String, Object> capture = match.capture();

        // Lowercase the keys as expected by hive
        Map<String, Object> lowerRoot = new HashMap();
        for (Map.Entry entry : capture.entrySet()) {
            lowerRoot.put(((String) entry.getKey()).toLowerCase(), entry.getValue());
        }
        capture = lowerRoot;

        Object value;
        for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
            try {
                value = capture.get(fieldName.toLowerCase()).toString();
            } catch (Exception e) {
                value = null;
            }
            row.add((String) value);
        }
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }
}