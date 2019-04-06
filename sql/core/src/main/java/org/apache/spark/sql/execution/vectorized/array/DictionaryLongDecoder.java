package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.column.Dictionary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class DictionaryLongDecoder implements DictionaryDecoder {
    public void appendValue(Dictionary dictionary,
                            WritableColumnVector arrayData,
                            WritableColumnVector dictionaryArray,
                            int index) {
        arrayData.appendLong(dictionary.decodeToLong(dictionaryArray.getInt(index)));
    }
}
