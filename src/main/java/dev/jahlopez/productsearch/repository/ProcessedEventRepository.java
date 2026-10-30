package dev.jahlopez.productsearch.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, UUID> {

    @Query("SELECT MAX(p.aggregateVersion) FROM ProcessedEventEntity p WHERE p.aggregateType = :aggregateType AND p.aggregateId = :aggregateId")
    Optional<Long> findHighestProcessedVersion(@Param("aggregateType") String aggregateType, @Param("aggregateId") String aggregateId);

    boolean existsByAggregateTypeAndAggregateIdAndAggregateVersion(
            String aggregateType, String aggregateId, Long aggregateVersion);
}