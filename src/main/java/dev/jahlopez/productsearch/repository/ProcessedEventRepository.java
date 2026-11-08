package dev.jahlopez.productsearch.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Queries the consumer inbox for event identity and aggregate-version checks used during
 * projection updates.
 */
@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, UUID> {

    /**
     * Finds the greatest inbox version recorded for one aggregate.
     *
     * @param aggregateType aggregate type stored by the consumer
     * @param aggregateId aggregate identifier
     * @return the maximum recorded version, or an empty result when no row exists
     */
    @Query("SELECT MAX(p.aggregateVersion) FROM ProcessedEventEntity p WHERE p.aggregateType = :aggregateType AND p.aggregateId = :aggregateId")
    Optional<Long> findHighestProcessedVersion(@Param("aggregateType") String aggregateType, @Param("aggregateId") String aggregateId);

    /**
     * Checks whether an inbox row already occupies an aggregate version.
     *
     * @param aggregateType aggregate type stored by the consumer
     * @param aggregateId aggregate identifier
     * @param aggregateVersion version to check
     * @return whether a matching row exists
     */
    boolean existsByAggregateTypeAndAggregateIdAndAggregateVersion(
            String aggregateType, String aggregateId, Long aggregateVersion);
}