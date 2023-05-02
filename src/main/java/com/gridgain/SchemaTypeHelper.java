package com.gridgain;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaTypeHelper {

    public static Set<Schema.Type> SUPPORTED_TYPES;

    static {
        SUPPORTED_TYPES =  new HashSet<Schema.Type>();
        SUPPORTED_TYPES.add(Schema.Type.BOOLEAN);
        SUPPORTED_TYPES.add(Schema.Type.BYTES);
        SUPPORTED_TYPES.add(Schema.Type.DOUBLE);
        SUPPORTED_TYPES.add(Schema.Type.ENUM);
        SUPPORTED_TYPES.add(Schema.Type.FLOAT);
        SUPPORTED_TYPES.add(Schema.Type.INT);
        SUPPORTED_TYPES.add(Schema.Type.LONG);
        SUPPORTED_TYPES.add(Schema.Type.STRING);
    }

    public static boolean shouldProcessField(Schema.Field field) {
        boolean result = false;
        Schema.Type currentType = field.schema().getType();
        if(Schema.Type.UNION.equals(currentType)) {
            List<Schema> subSchemas = field.schema().getTypes();
            int supportedCount = 0;
            for(Schema subSchema : subSchemas) {
                currentType = subSchema.getType();
                if(SUPPORTED_TYPES.contains(currentType)) {
                    ++supportedCount;
                }
            }
            if(supportedCount >= 1) {
                result = true;
            }
        } else {
            if(SUPPORTED_TYPES.contains(currentType)) {
                result = true;
            }
        }
        return result;
    }

    public static Schema.Type findConversionTypeFor(Schema.Field field) {
        Schema.Type result = null;
        Schema.Type currentType = field.schema().getType();
        if(Schema.Type.UNION.equals(currentType)) {
            List<Schema> subSchemas = field.schema().getTypes();
            for(Schema subSchema : subSchemas) {
                currentType = subSchema.getType();
                if(!Schema.Type.NULL.equals(currentType)) {
                    result = currentType;
                }
            }
        } else {
            if(!Schema.Type.NULL.equals(currentType)) {
                result = currentType;
            }
        }
        return result;
    }

    public static Object maybeConvert(Schema.Field currentField, Object value) {
        Object result = value;
        Schema.Type conversionType = SchemaTypeHelper.findConversionTypeFor(currentField);
        if(Schema.Type.BOOLEAN.equals(conversionType)) {
            // Untested at this point
            int intVal = (int) value;
            if(0 == intVal) {
                result = new Boolean(false);
            } else if(1 == intVal) {
                result = new Boolean(true);
            }
        } else if(Schema.Type.BYTES.equals(conversionType)) {
            // Untested at this point
            result = new String(((byte[]) value));
        } else if(Schema.Type.DOUBLE.equals(conversionType)) {
            // Untested at this point
            result = new Double((Double) value);
        } else if(Schema.Type.ENUM.equals(conversionType)) {
            // Untested at this point
            int intVal = (int) value;
            result = new Integer(intVal);
        } else if(Schema.Type.FLOAT.equals(conversionType)) {
            // Intentionally converting float to double in support of SQL functions such as min() & max()
            result = new Double(((Float) value).doubleValue());
        } else if(Schema.Type.INT.equals(conversionType)) {
            result = new Integer((Integer) value);
        } else if(Schema.Type.LONG.equals(conversionType)) {
            result = new Long((Long) value);
        } else if(Schema.Type.STRING.equals(conversionType)) {
            result = new String(((Utf8)value).toString());
        }
        return result;
    }

    public static String maybeCreateField(Schema.Field currentField) {
        String result = null;
        Schema.Type conversionType = SchemaTypeHelper.findConversionTypeFor(currentField);
        if(Schema.Type.BOOLEAN.equals(conversionType)) {
            result = currentField.name() + " int";
        } else if(Schema.Type.BYTES.equals(conversionType)) {
            result = currentField.name() + " varchar";
        } else if(Schema.Type.DOUBLE.equals(conversionType)) {
            result = currentField.name() + " double";
        } else if(Schema.Type.ENUM.equals(conversionType)) {
            result = currentField.name() + " int";
        } else if(Schema.Type.FLOAT.equals(conversionType)) {
            // Intentionally converting float to double in support of SQL functions such as min() & max()
            result = currentField.name() + " double";
        } else if(Schema.Type.INT.equals(conversionType)) {
            result = currentField.name() + " int";
        } else if(Schema.Type.LONG.equals(conversionType)) {
            result = currentField.name() + " long";
        } else if(Schema.Type.STRING.equals(conversionType)) {
            result = currentField.name() + " varchar";
        }
        return result;
    }

}
