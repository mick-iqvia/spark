package org.apache.spark.sql.execution.vectorized.array;

import org.apache.spark.sql.execution.datasources.parquet.VectorizedValuesReader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class VectorizedFloatArrayReader implements VectorizedArrayReader {
    private final VectorizedValuesReader vectorizedValuesReader;

    VectorizedFloatArrayReader(VectorizedValuesReader vectorizedValuesReader) {
        this.vectorizedValuesReader = vectorizedValuesReader;
    }

    public void readArray(int total, WritableColumnVector c) {
        vectorizedValuesReader.readFloatArray(total, c);
    }
}
