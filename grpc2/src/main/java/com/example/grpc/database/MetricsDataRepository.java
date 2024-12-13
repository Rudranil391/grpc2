package com.example.grpc.database;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetricsDataRepository extends JpaRepository<MetricsData, Long> {
}


