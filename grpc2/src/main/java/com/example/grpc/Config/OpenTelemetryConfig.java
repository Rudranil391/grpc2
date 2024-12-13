package com.example.grpc.Config;

import com.example.grpc.Utility.MetricsParquetWriter;
import com.example.grpc.Utility.MetricsParser;
import com.example.grpc.database.MetricsData;
import com.example.grpc.database.MetricsDataRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporters.inmemory.InMemorySpanExporter;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;


@Configuration
public class OpenTelemetryConfig {

    @Bean
    @Primary
    public InMemorySpanExporter inMemorySpanExporter() {
        return InMemorySpanExporter.create();
    }

    @Bean
    public JaegerGrpcSpanExporter jaegerGrpcSpanExporter() {
        return JaegerGrpcSpanExporter.builder()
                .setEndpoint("http://localhost:14250") // Jaeger gRPC endpoint
                .setTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Bean
    public PrometheusMeterRegistry prometheusMeterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Bean
    public Gauge registerGauges(PrometheusMeterRegistry meterRegistry) {
        return Gauge.builder("hello_service_2_active_requests", () -> getActiveRequestsCount())
                .description("Number of active requests to Hello Service 2")
                .register(meterRegistry);
    }

    private int getActiveRequestsCount() {
        // Logic to track active requests
        return 2; // Replace with actual count
    }


    @Bean
    @Primary
    public OpenTelemetry openTelemetry(InMemorySpanExporter spanExporter,JaegerGrpcSpanExporter jaegerExporter) {
        TextMapPropagator textMapPropagator = W3CTraceContextPropagator.getInstance();

        // Define a unique service name for the second service
        Resource resource = Resource.getDefault()
                .merge(Resource.builder()
                        .put(ResourceAttributes.SERVICE_NAME, "grpc-hello-service-2")
                        .put(ResourceAttributes.SERVICE_VERSION, "1.0.0")
                        .build());

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .addSpanProcessor(BatchSpanProcessor.builder(jaegerExporter).build())
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(textMapPropagator))
                .buildAndRegisterGlobal();
    }

    @Bean
    @Primary
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("com.example.grpc.HelloService2");
    }

    @Bean
    @Primary
    public GrpcTelemetry grpcTelemetry(OpenTelemetry openTelemetry) {
        return GrpcTelemetry.create(openTelemetry);
    }

    @RestController
    static class MetricsEndpoint {

        private final PrometheusMeterRegistry prometheusMeterRegistry;

        @Autowired
        public MetricsEndpoint(PrometheusMeterRegistry prometheusMeterRegistry) {
            this.prometheusMeterRegistry = prometheusMeterRegistry;
        }
        @Autowired
        private MetricsDataRepository metricsDataRepository;

        @GetMapping("/metrics")
        public String scrapeMetrics() throws IOException {
            String metricsText = prometheusMeterRegistry.scrape();
            //String metricsText1 = prometheusMeterRegistry.scrape();
            //System.out.println(metrics);

            List<MetricsParquetWriter.Metric> metrics = MetricsParser.parseMetrics(metricsText);
            MetricsParquetWriter writer = new MetricsParquetWriter();
            String path=writer.writeMetricsToParquet(metrics, "output/metrics.parquet");

            byte[] metricsData = Files.readAllBytes(Path.of(path));

            // Create a new MetricsData entity
            MetricsData metricsDataEntity = new MetricsData(metricsData, LocalDateTime.now(),"Service_2");

            // Save the entity to the database
            metricsDataRepository.save(metricsDataEntity);
            Files.delete(Path.of(path));

            return metricsText + "# EOF";
        }
    }

    @Bean
    public Meter meter(OpenTelemetry openTelemetry) {
        // Retrieve and return the Meter instance for creating metrics
        return openTelemetry.getMeter("com.example.grpc.HelloMetrics");
    }


}
