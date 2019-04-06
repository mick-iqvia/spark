package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class DictionaryBinaryDecoder implements DictionaryDecoder {
    public void appendValue(Dictionary dictionary,
                            WritableColumnVector arrayData,
                            WritableColumnVector dictionaryArray,
                            int index) {
        Binary v = dictionary.decodeToBinary(dictionaryArray.getDictId(index));
        arrayData.appendByteArray(v.getBytes(), 0, v.length());
    }
}
