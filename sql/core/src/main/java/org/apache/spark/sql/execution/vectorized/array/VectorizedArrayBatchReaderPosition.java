package org.apache.spark.sql.execution.vectorized.array;

public class VectorizedArrayBatchReaderPosition {
    static int STARTING_ROW_ID = -1;
    private final int rowsToRead;
    private int rowId = STARTING_ROW_ID;
    int valuesJustRead;
    int writeOffset;
    int lastWriteOffset;

    public VectorizedArrayBatchReaderPosition(int rowsToRead) {
        this.rowsToRead = rowsToRead;
    }

    public boolean batchDone() {
        return rowId == rowsToRead;
    }

    public int getValuesJustRead() {
        return valuesJustRead;
    }

    void incrRowId() {
        rowId += 1;
    }

    public int rowsToRead() {
        return rowsToRead;
    }

    public int getRowId() {
        return rowId;
    }

    void startReadLoop() {
        valuesJustRead = 0;
    }
}
