package com.github.k2sebeom.kafka.connect.transforms;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;

import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;


public class HeaderToKey<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = 
        "Extract header fields from the record and insert as key";

    public static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Field names on the headers to extract as the record key.");
 
    private List<String> fields;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
    
    @Override
    public R apply(R record) {
        Headers headers = record.headers();
        final Map<String, Object> key = new HashMap<>(fields.size());
        for(String field: fields) {
            Header header = headers.lastWithName(field);
            if(header != null) {
                key.put(field, header.value());
            }
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), null, key, record.valueSchema(), record.value(), record.timestamp(), record.headers());
    }
}