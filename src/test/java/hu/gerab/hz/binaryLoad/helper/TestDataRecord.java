package hu.gerab.hz.binaryLoad.helper;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

import java.util.Objects;

public class TestDataRecord implements Record<Data> {

    private Data value;

    public TestDataRecord(Data value) {
        this.value = value;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public void setValue(Data o) {
        value = o;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestDataRecord that = (TestDataRecord) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "TestDataRecord{" +
                "value=" + new String(value.toByteArray()) +
                '}';
    }

    @Override
    public Data getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onAccess(long now) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onUpdate(long now) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onStore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setVersion(long version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getCachedValueUnsafe() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTtl() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTtl(long ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastUpdateTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCreationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCreationTime(long creationTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getHits() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHits(long hits) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getExpirationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastStoredTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSequence(long sequence) {
        throw new UnsupportedOperationException();
    }
}
