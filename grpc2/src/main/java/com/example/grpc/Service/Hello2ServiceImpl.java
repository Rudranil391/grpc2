package com.example.grpc.Service;

import com.example.grpc.Hello2;
import com.example.grpc.HelloServiceGrpc;
import com.example.grpc.Utility.ParquetWriterUtil;
import com.example.grpc.database.TraceData;
import com.example.grpc.database.TraceDataRepository;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
public class Hello2ServiceImpl extends HelloServiceGrpc.HelloServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(Hello2ServiceImpl.class);
    private final Tracer tracer;
    private final GrpcTelemetry grpcTelemetry;
    private final MeterRegistry meterRegistry; // Add MeterRegistry as a field
    private final LongCounter requestCounter;
    private double activeRequestsCount; // Field to hold active request count

    @Autowired
    private TraceDataRepository traceDataRepository;

    @Autowired
    public Hello2ServiceImpl(Tracer tracer, GrpcTelemetry grpcTelemetry, MeterRegistry meterRegistry, Meter meter) {
        this.tracer = tracer;
        this.grpcTelemetry = grpcTelemetry;
        this.meterRegistry = meterRegistry;  // Use .gauge() or .counter() as appropriate
        this.requestCounter = meter.counterBuilder("hello_service_requests")
                .setDescription("Number of requests to Hello Service")
                .setUnit("1")
                .build();

    }

    @Override
    public void sayHello(com.example.grpc.Hello2.HelloRequest request, StreamObserver<com.example.grpc.Hello2.HelloResponse> responseObserver) {
        // Start a new span to continue the trace
        activeRequestsCount++;
        Span span = tracer.spanBuilder("sayHello - Service 2").startSpan();
        Span serverSpan = Span.current();

        try (Scope scope = span.makeCurrent()) {
            // Log processing details and add an attribute
            requestCounter.add(1);
            logger.info("Processing sayHello request in Service 2 for name: {}", request.getName());
            span.setAttribute(AttributeKey.stringKey("processingService"), "Service 2");

            // Generate a response
            String message = "Hello from Service 2, " + request.getName();
            com.example.grpc.Hello2.HelloResponse response = Hello2.HelloResponse.newBuilder()
                    .setMessage(message)
                    .build();

            // Send response back to Service 1
            span.setAttribute(AttributeKey.stringKey("responseMessageKey2"), response.getMessage());

            System.out.println(span);
            System.out.println(serverSpan);

            // Collect spans and convert to GenericRecord
            List<GenericRecord> records = new ArrayList<>();
            if (span instanceof ReadableSpan) {
                SpanData spanData = ((ReadableSpan) span).toSpanData();
                GenericRecord record = new GenericData.Record(ParquetWriterUtil.SCHEMA);
                record.put("traceId", spanData.getTraceId());
                record.put("spanId", spanData.getSpanId());
                record.put("name", spanData.getName());
                record.put("attributes", spanData.getAttributes().toString());
                records.add(record);
            }

            if (serverSpan instanceof ReadableSpan) {
                SpanData spanData = ((ReadableSpan) serverSpan).toSpanData();
                GenericRecord record = new GenericData.Record(ParquetWriterUtil.SCHEMA);
                record.put("traceId", spanData.getTraceId());
                record.put("spanId", spanData.getSpanId());
                record.put("name", spanData.getName());
                record.put("attributes", spanData.getAttributes().toString());
                records.add(record);
            }

            // Write records to Parquet
            byte[] parquetData = ParquetWriterUtil.writeSpansToParquet(records);
            TraceData traceData = new TraceData(parquetData, LocalDateTime.now(),"service_2");
            traceDataRepository.save(traceData);

            logger.info("Spans written to Parquet format and saved to MySQL as blob");



            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error processing sayHello request in Service 2", e);
            responseObserver.onError(e);
        } finally {
            // End the span
            span.end();
        }
    }
}
