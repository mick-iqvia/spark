package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.util.TimeZone;

public interface DictionaryDecoder {
    void appendValue(Dictionary dictionary,
                     WritableColumnVector arrayData,
                     WritableColumnVector dictionaryIds,
                     int index);

    static DictionaryDecoder createDictionaryDecoder(PrimitiveType.PrimitiveTypeName typeName, TimeZone convertTz) {
        switch (typeName) {
            case INT32:
                return new DictionaryIntDecoder();
            case INT64:
                return new DictionaryLongDecoder();
            case INT96:
                return new DictionaryTimestampDecoder(convertTz);
            case FLOAT:
                return new DictionaryFloatDecoder();
            case DOUBLE:
                return new DictionaryDoubleDecoder();
            case BINARY:
                return new DictionaryBinaryDecoder();
        }
        throw new IllegalStateException("Unsupported primitive type for vectorized dictionary decoder: " + typeName);
    }
}
