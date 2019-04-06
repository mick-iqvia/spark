package org.apache.spark.sql.execution.vectorized.array;

import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.datasources.parquet.ParquetRowConverter;
import org.apache.spark.sql.execution.vectorized.Dictionary;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.TimeZone;

public class DictionaryColumnVectorFacade extends ColumnVector {
    private static final TimeZone UTC = DateTimeUtils.TimeZoneUTC();
    private final ColumnVector delegate;
    private final Dictionary dictionary;
    private final boolean isTimestamp;
    private final TimeZone convertTz;

    public DictionaryColumnVectorFacade(DataType type, ColumnVector delegate, Dictionary dictionary, TimeZone convertTz) {
        super(type);
        this.delegate = delegate;
        this.dictionary = dictionary;
        this.convertTz = convertTz;
        isTimestamp = type instanceof ArrayType && ((ArrayType)type).elementType() instanceof TimestampType;
    }
    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean hasNull() {
        return delegate.hasNull();
    }

    @Override
    public int numNulls() {
        return delegate.numNulls();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return delegate.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int rowId) {
        return (short)dictionary.decodeToInt(delegate.getInt(rowId));
    }

    @Override
    public int getInt(int rowId) {
        return dictionary.decodeToInt(delegate.getInt(rowId));
    }

    @Override
    public long getLong(int rowId) {
        if (isTimestamp) {
            if (convertTz != null && !convertTz.equals(UTC)) {
                Binary binary = Binary.fromConstantByteArray(dictionary.decodeToBinary(delegate.getInt(rowId)));
                long rawTime = ParquetRowConverter.binaryToSQLTimestamp(binary);
                return DateTimeUtils.convertTz(rawTime, convertTz, UTC);
            } else {
                Binary binary = Binary.fromConstantByteArray(dictionary.decodeToBinary(delegate.getInt(rowId)));
                return ParquetRowConverter.binaryToSQLTimestamp(binary);
            }
        } else {
            return dictionary.decodeToLong(delegate.getInt(rowId));
        }
    }

    @Override
    public float getFloat(int rowId) {
        return dictionary.decodeToFloat(delegate.getInt(rowId));
    }

    @Override
    public double getDouble(int rowId) {
        return dictionary.decodeToDouble(delegate.getInt(rowId));
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        byte[] bytes = dictionary.decodeToBinary(delegate.getInt(rowId));
        return UTF8String.fromBytes(bytes);
    }

    @Override
    public byte[] getBinary(int rowId) {
        return dictionary.decodeToBinary(delegate.getInt(rowId));
    }

    @Override
    protected ColumnVector getChild(int ordinal) {
        throw new UnsupportedOperationException();
    }
}
