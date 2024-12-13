package com.example.grpc.Utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class MetricsParquetWriter {

    // Define Parquet schema for metrics data
    private static final MessageType METRIC_SCHEMA = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("type")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("description")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("value")
            .named("Metric");

    public String writeMetricsToParquet(List<Metric> metrics, String outputPath) throws IOException {
        String uniqueOutputPath = outputPath.replace(".parquet", "_" + System.currentTimeMillis() + ".parquet");

        // Use LocalFileOutput to wrap the file path
        LocalFileOutput outputFile = new LocalFileOutput(Paths.get(uniqueOutputPath));

        GroupWriteSupport.setSchema(METRIC_SCHEMA, new Configuration());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(METRIC_SCHEMA)
                .build()) {
            for (Metric metric : metrics) {
                Group group = new SimpleGroup(METRIC_SCHEMA);
                group.add("name", metric.getName());
                group.add("type", metric.getType());
                group.add("description", metric.getDescription());
                group.add("value", metric.getValue());
                //System.out.println(metric.getType() + "" + metric.getValue());
                writer.write(group);
            }
        }
        return uniqueOutputPath;
    }

//    // Read metrics from Parquet file and return a list of Metric objects
//    public List<Metric> readFromParquet(String parquetFilePath) throws IOException {
//        Path nioPath = Paths.get(parquetFilePath);  // Correct usage of java.nio.file.Path
//
//        // Convert java.nio.file.Path to org.apache.hadoop.fs.Path
//        Path hadoopPath = new Path(nioPath.toUri());
//
//        // Open the Parquet file using Hadoop Path
//        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), hadoopPath).build()) {
//            List<Metric> metrics = new ArrayList<>();
//            Group group;
//            while ((group = reader.read()) != null) {
//                String name = group.getString("name", 0);
//                String type = group.getString("type", 0);
//                String description = group.getString("description", 0);
//                double value = group.getDouble("value", 0);
//
//                // Create Metric object from the Parquet record
//                metrics.add(new Metric(name, type, description, value));
//            }
//            return metrics;
//        }
//    }

    // Example Metric class to encapsulate parsed metrics data
    public static class Metric {
        private final String name;
        private final String type;
        private final String description;
        private final double value;

        public Metric(String name, String type, String description, double value) {
            this.name = name;
            this.type = type;
            this.description = description;
            this.value = value;
        }

        public String getName() { return name; }
        public String getType() { return type; }
        public String getDescription() { return description; }
        public double getValue() { return value; }
    }

    public class LocalFileOutput implements OutputFile {
        private final Path filePath;

        public LocalFileOutput(Path filePath) {
            this.filePath = filePath;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            OutputStream outputStream = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            return new LocalPositionOutputStream(outputStream);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            // If you want to overwrite the file, just use CREATE and WRITE
            OutputStream outputStream = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            return new LocalPositionOutputStream(outputStream);
        }

        @Override
        public boolean supportsBlockSize() {
            return false; // Simple implementation doesn't require block size
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        // PositionOutputStream implementation that works with OutputStream
        public static class LocalPositionOutputStream extends PositionOutputStream {
            private final OutputStream outputStream;
            private long position = 0;

            public LocalPositionOutputStream(OutputStream outputStream) {
                this.outputStream = outputStream;
            }

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) throws IOException {
                outputStream.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
                position += len;
            }

            @Override
            public void close() throws IOException {
                outputStream.close();
            }
        }
    }
}
