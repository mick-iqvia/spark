package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.column.Dictionary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class DictionaryFloatDecoder implements DictionaryDecoder{
    public void appendValue(Dictionary dictionary,
                            WritableColumnVector arrayData,
                            WritableColumnVector dictionaryArray,
                            int index) {
        arrayData.appendFloat(dictionary.decodeToFloat(dictionaryArray.getInt(index)));
    }
}
