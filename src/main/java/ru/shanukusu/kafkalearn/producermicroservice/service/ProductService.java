package ru.shanukusu.kafkalearn.producermicroservice.service;

import ru.shanukusu.kafkalearn.producermicroservice.service.dto.CreateProductDto;

import java.util.concurrent.ExecutionException;

public interface ProductService {
    String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException;
}
