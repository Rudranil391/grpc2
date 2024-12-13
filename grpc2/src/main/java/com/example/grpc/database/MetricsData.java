package com.example.grpc.database;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "metrics")
public class MetricsData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Lob
    @Column(name = "metrics_data", columnDefinition = "LONGBLOB", nullable = false)
    private byte[] metricsData;

    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;

    @Column(name = "metric_system", nullable = false)  // New column
    private String metricSystem;

    public MetricsData() {}

    public MetricsData(byte[] metricsData, LocalDateTime timestamp, String metricSystem) {
        this.metricsData = metricsData;
        this.timestamp = timestamp;
        this.metricSystem = metricSystem;
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public byte[] getMetricsData() {
        return metricsData;
    }

    public void setMetricsData(byte[] metricsData) {
        this.metricsData = metricsData;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getMetricSystem() {
        return metricSystem;
    }

    public void setMetricSystem(String metricSystem) {
        this.metricSystem = metricSystem;
    }
}
