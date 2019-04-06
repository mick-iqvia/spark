package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.column.Dictionary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class DictionaryIntDecoder implements DictionaryDecoder{
    public void appendValue(Dictionary dictionary,
                            WritableColumnVector arrayData,
                            WritableColumnVector dictionaryArray,
                            int index) {
        arrayData.appendInt(dictionary.decodeToInt(dictionaryArray.getInt(index)));
    }
}
