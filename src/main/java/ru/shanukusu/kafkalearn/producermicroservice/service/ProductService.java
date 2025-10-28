package ru.shanukusu.kafkalearn.producermicroservice.service;

import ru.shanukusu.kafkalearn.producermicroservice.service.dto.CreateProductDto;

public interface ProductService {
    String createProduct(CreateProductDto createProductDto);
}
