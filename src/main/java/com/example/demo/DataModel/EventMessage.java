package com.example.demo.DataModel;

import java.time.Instant;
import java.util.UUID;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record EventMessage(
    @NotNull UUID id,
    @NotBlank @Size(max = 100) String type,
    @NotBlank @Size(max = 200) String description,
    @NotBlank @Size(max = 4096) String payload,
    @NotBlank @Size(max = 100) String createdBy,
    @NotNull Instant createdAt
) {}
