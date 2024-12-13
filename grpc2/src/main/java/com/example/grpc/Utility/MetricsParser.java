package com.example.grpc.Utility;

import java.util.ArrayList;
import java.util.List;

public class MetricsParser {

    public static List<MetricsParquetWriter.Metric> parseMetrics(String metricsText) {
        List<MetricsParquetWriter.Metric> metrics = new ArrayList<>();
        String[] lines = metricsText.split("\n");
        String name = null, type = null, description = null;

        for (String line : lines) {
            line = line.trim();
            if (line.startsWith("# HELP")) {
                String[] helpParts = line.split(" ", 3);
                name = helpParts[1];
                description = helpParts[2];
            } else if (line.startsWith("# TYPE")) {
                type = line.split(" ")[2];
            } else if (!line.startsWith("#")) {
                String[] parts = line.split(" ");
                name = parts[0];

                // Check if the value is numeric before parsing
                if (parts.length > 1 && isNumeric(parts[1])) {
                    double value = Double.parseDouble(parts[1]);
                    metrics.add(new MetricsParquetWriter.Metric(name, type, description, value));
                } else {
                    System.err.println("Non-numeric metric value encountered: " + parts[1]);
                    // Optionally log or skip this metric
                }
            }
        }
        System.out.println("we have parsed correctly");
        return metrics;
    }

    public static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}

