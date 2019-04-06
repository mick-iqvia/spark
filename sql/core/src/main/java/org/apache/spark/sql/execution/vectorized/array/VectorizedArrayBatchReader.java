package org.apache.spark.sql.execution.vectorized.array;

import org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class VectorizedArrayBatchReader {
    private VectorizedRleValuesReader rlReader;
    private VectorizedRleValuesReader dlReader;
    public VectorizedArrayBatchReader(VectorizedRleValuesReader rlReader, VectorizedRleValuesReader dlReader) {
       this.rlReader = rlReader;
       this.dlReader = dlReader;
    }

    public void readArray(
            VectorizedArrayBatchReaderPosition position,
            VectorizedArrayReader arrayReader,
            WritableColumnVector arrayWriter,
            WritableColumnVector nullWriter,
            final int valuesLeftInPage,
            final int nonNullLevel,
            final int nullArrayElementLevel) {
        final int startRowId = position.getRowId();
        position.valuesJustRead = 0;
        position.startReadLoop();
        while (position.valuesJustRead < valuesLeftInPage) {
            advanceLevelReaders();
            int currentValuesToRead = Math.min(Math.min(dlReader.getCurrentCount(), rlReader.getCurrentCount()), valuesLeftInPage - position.valuesJustRead);
            if  (dlReader.isRle()) {
                if (rlReader.isRle()) {
                    if (notNullRun(nonNullLevel)) {
                        if (newRowRun()) {
                            for (int levelsIndex = 0; levelsIndex < currentValuesToRead; ++levelsIndex) {
                                processNewRow(position, arrayWriter, nullWriter, arrayReader, startRowId, levelsIndex, nonNullLevel, nullArrayElementLevel,
                                        dlReader.getCurrentValue());
                                if (position.batchDone()) return;
                            }
                        } else {
                            position.writeOffset += currentValuesToRead;
                        }
                    } else {
                        if (newRowRun()) {
                            for (int levelsIndex = 0; levelsIndex < currentValuesToRead; ++levelsIndex) {
                                processNewRow(position, arrayWriter, nullWriter, arrayReader, startRowId, levelsIndex, nonNullLevel, nullArrayElementLevel,
                                        dlReader.getCurrentValue());
                                if (position.batchDone()) return;
                            }
                        } else {
                            addNullValues(position, arrayWriter, arrayReader, currentValuesToRead);
                        }
                    }
                } else {
                    if (notNullRun(nonNullLevel)) {
                        int levelsIndex = 0;
                        while (levelsIndex < currentValuesToRead) {
                            while (levelsIndex < currentValuesToRead && newRow()) {
                                processNewRow(position, arrayWriter, nullWriter, arrayReader, startRowId, levelsIndex, nonNullLevel, nullArrayElementLevel,
                                        dlReader.getCurrentValue());
                                if (position.batchDone()) return;

                                rlReader.incrBufferIndex();
                                levelsIndex++;
                            }
                            while (levelsIndex < currentValuesToRead && !newRow()) {
                                position.writeOffset++;

                                rlReader.incrBufferIndex();
                                levelsIndex++;
                            }
                        }
                    } else {
                        int levelsIndex = 0;
                        while (levelsIndex < currentValuesToRead) {
                            while (levelsIndex < currentValuesToRead && newRow()) {
                                processNewRow(position, arrayWriter, nullWriter, arrayReader, startRowId, levelsIndex, nonNullLevel, nullArrayElementLevel,
                                        dlReader.getCurrentValue());
                                if (position.batchDone()) return;

                                rlReader.incrBufferIndex();
                                levelsIndex++;

                            }
                            int numNulls = 0;
                            while (levelsIndex < currentValuesToRead && !newRow()) {
                                numNulls++;

                                rlReader.incrBufferIndex();
                                levelsIndex++;
                            }
                            if (numNulls > 0) addNullValues(position, arrayWriter, arrayReader, numNulls);
                        }
                    }
                }
            } else {
                if (rlReader.isRle()) {
                    for (int levelsIndex = 0; levelsIndex < currentValuesToRead; ++levelsIndex) {
                        if (newRowRun()) {
                            processNewRow(position, arrayWriter, nullWriter, arrayReader, startRowId, levelsIndex, nonNullLevel, nullArrayElementLevel,
                                    dlReader.getBufferValue());
                            if (position.batchDone()) return;
                        } else {
                            if (notNull(nonNullLevel)) position.writeOffset++;
                            else addNullValues(position, arrayWriter, arrayReader, 1);
                        }
                        dlReader.incrBufferIndex();
                    }
                } else {
                    for (int levelsIndex = 0; levelsIndex < currentValuesToRead; ++levelsIndex) {
                        if (newRow()) {
                            processNewRow(position, arrayWriter, nullWriter, arrayReader, startRowId, levelsIndex, nonNullLevel, nullArrayElementLevel,
                                    dlReader.getBufferValue());
                            if (position.batchDone()) return;
                        } else {
                            if (notNull(nonNullLevel)) position.writeOffset++;
                            else addNullValues(position, arrayWriter, arrayReader, 1);
                        }

                        rlReader.incrBufferIndex();
                        dlReader.incrBufferIndex();
                    }
                }
            }
            adjustPositions(position, currentValuesToRead);
        }
        // We are here because we have exhausted page data, there may still be data for this row on the next page
        calculateArrayLengthsAndAdjustPosition(position, startRowId, arrayWriter, 0);
        readValues(position, arrayWriter, arrayReader);
    }

    private void adjustPositions(VectorizedArrayBatchReaderPosition position, int adjustment) {
        if (adjustment != 0) {
            position.valuesJustRead += adjustment;
            dlReader.adjustCurrentCount(-adjustment);
            rlReader.adjustCurrentCount(-adjustment);
        }
    }

    private void advanceLevelReaders() {
        if (rlReader.getCurrentCount() == 0) {
            rlReader.readNextGroup();
        }
        if (dlReader.getCurrentCount() == 0) {
            dlReader.readNextGroup();
        }
    }

    private void processNewRow(VectorizedArrayBatchReaderPosition position,
                               WritableColumnVector arrayWriter,
                               WritableColumnVector nullWriter,
                               VectorizedArrayReader arrayReader,
                               int startRowId,
                               int adjustPosition,
                               int nonNullLevel,
                               int nullArrayElementLevel,
                               int defLevel) {
        position.incrRowId();
        if (position.batchDone()) {
            calculateArrayLengthsAndAdjustPosition(position, startRowId, arrayWriter, adjustPosition);
            readValues(position, arrayWriter, arrayReader);
        } else {
            arrayWriter.setArrayOffset(position.getRowId(), position.writeOffset);
            if (isNonNullElement(defLevel, nonNullLevel)) {
                position.writeOffset++;
            } else if (isNullElement(defLevel, nullArrayElementLevel)) {
                addNullValues(position, arrayWriter, arrayReader, 1);
            } else if (isNullArray(defLevel, nullArrayElementLevel, nonNullLevel)) {
                nullWriter.putNull(position.getRowId());
            } // otherwise empty array, nothing to do
        }
    }

    // Regarding def levels.
    // def level == nonNullLevel, which is the max level, means element defined
    // def level == nullArrayElementLevel, only for arrays with nullable elements, means null element,
    //              nullArrayElementLevel is -1 for arrays with non-nullable elements
    // def level == 1 less than nullArrayElementLevel or nonNullLevel for arrays with non-nullable elements means empty array
    // def level == 1 less than above means null array
    private boolean isNonNullElement(int defLevel, int nonNullLevel) {
        return defLevel == nonNullLevel;
    }
    private boolean isNullElement(int defLevel, int nullArrayElementLevel) {
        return defLevel == nullArrayElementLevel;
    }

    private boolean isNullArray(int defLevel, int nullArrayElementLevel, int nonNullLevel) {
        if (nullArrayElementLevel == -1) { // non-nullable elements
            return defLevel == nonNullLevel - 2;
        } else {
            return defLevel == nonNullLevel - 3;
        }
    }

    private void readValues(VectorizedArrayBatchReaderPosition position, WritableColumnVector arrayWriter, VectorizedArrayReader arrayReader) {
        int numValuesToRead = position.writeOffset - position.lastWriteOffset;
        if (numValuesToRead > 0) {
            arrayReader.readArray(numValuesToRead, arrayWriter);
            position.lastWriteOffset = position.writeOffset;
        }
    }

    private void addNullValues(VectorizedArrayBatchReaderPosition position, WritableColumnVector arrayWriter, VectorizedArrayReader data, int numNulls) {
        readValues(position, arrayWriter, data);
        arrayWriter.arrayData().appendNulls(numNulls);
        position.writeOffset += numNulls;
        position.lastWriteOffset = position.writeOffset;
    }

    private boolean newRow() {
        return rlReader.getBufferValue() == 0;
    }

    private boolean newRowRun() {
        return rlReader.getCurrentValue() == 0;
    }

    private boolean notNullRun(int nonNullLevel) {
        return dlReader.getCurrentValue() == nonNullLevel;
    }

    private boolean notNull(int nonNullLevel) {
        return dlReader.getBufferValue() == nonNullLevel;
    }

    private void calculateArrayLengthsAndAdjustPosition(VectorizedArrayBatchReaderPosition position,
                                                        int startRowId,
                                                        WritableColumnVector c,
                                                        int adjustment) {

        adjustPositions(position, adjustment);

        int endRowId;
        if (position.batchDone()) endRowId = position.getRowId() - 1;
        else endRowId = position.getRowId();

        if (startRowId == VectorizedArrayBatchReaderPosition.STARTING_ROW_ID) {
            startRowId = 0;
        }

        c.calculateArrayLengths(startRowId, endRowId, position.writeOffset);
    }

}
