package org.apache.spark.sql.execution.vectorized.array;

import org.apache.spark.sql.execution.datasources.parquet.VectorizedValuesReader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.util.TimeZone;

public class VectorizedTimestampArrayReader implements VectorizedArrayReader {
    private final VectorizedValuesReader vectorizedValuesReader;
    private final TimeZone convertTz;

    VectorizedTimestampArrayReader(VectorizedValuesReader vectorizedValuesReader, TimeZone convertTz) {
        this.vectorizedValuesReader = vectorizedValuesReader;
        this.convertTz = convertTz;
    }

    public void readArray(int total, WritableColumnVector c) {
        vectorizedValuesReader.readTimestampArray(total, c, convertTz);
    }
}
