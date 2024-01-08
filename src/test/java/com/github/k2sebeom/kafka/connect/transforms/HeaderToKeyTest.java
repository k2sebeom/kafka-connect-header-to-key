package com.github.k2sebeom.kafka.connect.transforms;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

public class HeaderToKeyTest {
    private HeaderToKey<SourceRecord> xform = new HeaderToKey<SourceRecord>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test
    public void schemaLessKeyInsertion() {
        final Map<String, Object> props = new HashMap<>();
        props.put("fields", "foo");
        
        xform.configure(props);

        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("foo", "bar");
        
        final SourceRecord record = new SourceRecord(
            null,
            null,
            "test",
            0,
            null,
            "testkey",
            null,
            "testValue",
            0L,
            headers
        );

        final SourceRecord transformedRecord = xform.apply(record);
        @SuppressWarnings("unchecked")
        Map<String, String> keyMap = (Map<String, String>)transformedRecord.key();

        assertEquals("bar", keyMap.get("foo"));
    }
}
