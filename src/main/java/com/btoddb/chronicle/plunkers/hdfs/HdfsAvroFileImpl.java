package com.btoddb.chronicle.plunkers.hdfs;

import com.btoddb.chronicle.Event;
import com.btoddb.chronicle.serializers.EventSerializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 *
 */
public class HdfsAvroFileImpl extends HdfsFileBaseImpl {
    private static final Logger logger = LoggerFactory.getLogger(HdfsAvroFileImpl.class);

    private static final String SCHEMA_JSON = "{" +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"headers\", " +
            "      \"type\": {" +
            "        \"type\": \"map\", \"avro.java.string\": \"String\", " +
            "        \"values\": {" +
            "          \"type\": \"string\", \"avro.java.string\": \"String\"" +
            "        }" +
            "      }" +
            "    }, " +
            "    {" +
            "      \"name\": \"body\", " +
            "      \"type\": {" +
            "        \"type\": \"string\", \"avro.java.string\": \"String\"" +
            "      }" +
            "    }" +
            "  ], " +
            "  \"name\": \"Chronicle\", " +
            "  \"type\": \"record\"" +
            "}";
    private static final Schema SCHEMA_AVRO = new Schema.Parser().parse(SCHEMA_JSON);

    private DataFileWriter<Object> dataFileWriter;

    private int syncIntervalBytes = 2*1024*1024;
    private String compressionCodec = "none";


    @Override
    public void init(String permFilename, String openFilename, EventSerializer serializer) throws IOException {
        super.init(permFilename, openFilename, serializer);

        dataFileWriter = null;
        DatumWriter<Object> writer = new ReflectDatumWriter<>();
        dataFileWriter = new DataFileWriter<Object>(writer);


        dataFileWriter.setSyncInterval(syncIntervalBytes);

//        try {
//            CodecFactory codecFactory = CodecFactory.fromString(compressionCodec);
//            dataFileWriter.setCodec(codecFactory);
//        } catch (AvroRuntimeException e) {
//            logger.warn("", e);
//        }

        dataFileWriter.create(SCHEMA_AVRO, outputStream);
    }

    @Override
    public void writeInternal(Event event) throws IOException {
        dataFileWriter.append(serializer.convert(event));
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
        super.close();
    }

    @Override
    public void flush() throws IOException {
        // gotta flush DataFileWriter otherwise Avro data will not be written until sync interval is hit
        dataFileWriter.flush();
        super.flush();
    }

}
