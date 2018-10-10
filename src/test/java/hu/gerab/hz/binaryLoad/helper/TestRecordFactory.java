package hu.gerab.hz.binaryLoad.helper;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;


public class TestRecordFactory implements RecordFactory<Data> {

    @Override
    public Record<Data> newRecord(Object value) {
        if(value instanceof Data){
            return new TestDataRecord((Data) value);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValue(Record<Data> record, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        throw new UnsupportedOperationException();
    }
}
