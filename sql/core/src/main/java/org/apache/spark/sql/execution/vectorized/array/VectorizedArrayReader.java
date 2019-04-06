package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedValuesReader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.util.TimeZone;

public interface VectorizedArrayReader {
    void readArray(int total, WritableColumnVector c);
    static VectorizedArrayReader createArrayReader(PrimitiveType.PrimitiveTypeName typeName, VectorizedValuesReader vectorizedValuesReader, TimeZone convertTz) {
        switch (typeName) {
            case INT32:
                return new VectorizedIntArrayReader(vectorizedValuesReader);
            case INT64:
                return new VectorizedLongArrayReader(vectorizedValuesReader);
            case INT96:
                return new VectorizedTimestampArrayReader(vectorizedValuesReader, convertTz);
            case FLOAT:
                return new VectorizedFloatArrayReader(vectorizedValuesReader);
            case DOUBLE:
                return new VectorizedDoubleArrayReader(vectorizedValuesReader);
            case BINARY:
                return new VectorizedBinaryArrayReader(vectorizedValuesReader);
        }
        throw new IllegalStateException("Unsupported primitive type for vectorized array reader: " + typeName);
    }

    static VectorizedArrayReader createDictionaryArrayReader(VectorizedValuesReader vectorizedValuesReader) {
        return new VectorizedIntArrayReader(vectorizedValuesReader);
    }

    }
