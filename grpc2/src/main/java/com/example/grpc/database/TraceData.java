package com.example.grpc.database;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "traces")
public class TraceData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Lob
    @Column(name = "trace_data", columnDefinition = "LONGBLOB", nullable = false)
    private byte[] traceData;

    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;

    @Column(name = "trace_system", nullable = false)  // New column
    private String TraceSystem;

    public TraceData() {}

    public TraceData(byte[] traceData, LocalDateTime timestamp,String TraceSystem) {
        this.traceData = traceData;
        this.timestamp = timestamp;
        this.TraceSystem=TraceSystem;
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public byte[] getTraceData() {
        return traceData;
    }

    public void setTraceData(byte[] traceData) {
        this.traceData = traceData;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getTraceSystem() {
        return TraceSystem;
    }

    public void setTraceSystem(String metricSystem) {
        this.TraceSystem = metricSystem;
    }
}


