package com.example.grpc.Utility;

import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.List;

import static java.lang.System.out;

@Component
public class ParquetWriterUtil {

    public static final Schema SCHEMA = new Schema.Parser().parse("{\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"SpanData\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"traceId\", \"type\": \"string\"},\n" +
            "     {\"name\": \"spanId\", \"type\": \"string\"},\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"attributes\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}");


    public static byte[] writeSpansToParquet(List<GenericRecord> records) throws IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(byteArrayOutputStream))
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .build()) {

            for (GenericRecord record : records) {
                writer.write(record);
            }
        }

        return byteArrayOutputStream.toByteArray();
    }


    public static void readSpansFromParquet(String filePath) {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                out.println("Read record: " + record);
            }
        } catch (IOException e) {
            e.printStackTrace(); // Handle exceptions appropriately
        }
    }


    // Helper class to provide a Parquet-compatible OutputFile wrapper around an OutputStream.
    public static class LocalOutputFile implements OutputFile {
        private final ByteArrayOutputStream byteArrayOutputStream;

        public LocalOutputFile(ByteArrayOutputStream byteArrayOutputStream) {
            this.byteArrayOutputStream = byteArrayOutputStream;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return new PositionOutputStream() {
                private long position = 0;

                @Override
                public long getPos() {
                    return position; // Track position in the byte array
                }

                @Override
                public void write(int b) throws IOException {
                    byteArrayOutputStream.write(b);
                    position++; // Increment position after each byte write
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    byteArrayOutputStream.write(b, off, len);
                    position += len; // Increment position by the number of bytes written
                }

                @Override
                public void close() throws IOException {
                    // No need to close the byteArrayOutputStream explicitly
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            // Just call create, as there’s no overwriting needed for in-memory stream
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}



