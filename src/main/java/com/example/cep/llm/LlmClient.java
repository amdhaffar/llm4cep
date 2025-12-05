package com.example.cep.llm;

import java.util.List;

/**
 * High level interface for an LLM client responsible for generating business rules
 * and translating them into CEP descriptors.
 */
public interface LlmClient {
    /**
     * Generates natural language business rules for the demo use case.
     */
    List<String> generateNaturalLanguageRules();

    /**
     * Translates a natural language rule into a structured CEP descriptor.
     */
    CepPatternDescriptor generateCepPatternDescriptor(String naturalLanguageRule);
}
