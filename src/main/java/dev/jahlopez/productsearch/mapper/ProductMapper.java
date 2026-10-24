package dev.jahlopez.productsearch.mapper;

import dev.jahlopez.productsearch.document.ProductDocument;
import dev.jahlopez.productsearch.dto.ProductResponseDto;
import dev.jahlopez.productsearch.event.ProductEvent;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

/**
 * Maps between OpenSearch product documents, API response DTOs,
 * and incoming Kafka product events.
 */
@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface ProductMapper {

    /**
     * Converts an OpenSearch product document to a response DTO.
     */
    ProductResponseDto toResponseDto(ProductDocument document);

    /**
     * Converts a response DTO to an OpenSearch document.
     * The DTO does not own the document version, so it is not mapped here.
     */
    @Mapping(target = "version", ignore = true)
    ProductDocument toDocument(ProductResponseDto responseDto);

    /**
     * Converts a Kafka product event to an OpenSearch document.
     */
    @Mapping(target = "id", source = "productId")
    ProductDocument toDocument(ProductEvent event);
}