package com.example.grpc.Service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class GrpcServerRunner implements CommandLineRunner {

    private final int port = 50052; // Use a different port for Service 2
    private Server server;
    private final GrpcTelemetry grpcTelemetry;
    private final Hello2ServiceImpl hello2Service;

    @Autowired
    public GrpcServerRunner(GrpcTelemetry grpcTelemetry, Hello2ServiceImpl hello2Service) {
        this.grpcTelemetry = grpcTelemetry;
        this.hello2Service = hello2Service;
    }

    @Override
    public void run(String... args) throws Exception {
        start();
        System.out.println("gRPC server for Service 2 started on port: " + port);
    }

    private void start() throws Exception {
        server = ServerBuilder.forPort(port)
                .addService(hello2Service)
                .intercept(grpcTelemetry.newServerInterceptor())
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down gRPC server for Service 2");
            stop();
            System.err.println("Server for Service 2 shut down");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}

