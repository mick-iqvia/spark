package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetRowConverter;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.util.TimeZone;

public class DictionaryTimestampDecoder implements DictionaryDecoder {
    private static final TimeZone UTC = DateTimeUtils.TimeZoneUTC();
    private final TimeZone convertTz;
    DictionaryTimestampDecoder(TimeZone convertTz) {
        this.convertTz = convertTz;
    }

    public void appendValue(Dictionary dictionary,
                            WritableColumnVector arrayData,
                            WritableColumnVector dictionaryArray,
                            int index) {
        if (convertTz != null && !convertTz.equals(UTC)) {
            Binary v = dictionary.decodeToBinary(dictionaryArray.getInt(index));
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(v);
            long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
            arrayData.appendLong(adjTime);
        } else {
            Binary v = dictionary.decodeToBinary(dictionaryArray.getInt(index));
            arrayData.appendLong(ParquetRowConverter.binaryToSQLTimestamp(v));
        }
    }
}
