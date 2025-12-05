package com.example.cep.llm;

import org.apache.flink.cep.pattern.Pattern;

/**
 * Represents a fully compiled rule ready to be applied to a stream.
 */
public class CompiledCepRule<T> {
    private final String naturalLanguageRule;
    private final CepPatternDescriptor descriptor;
    private final Pattern<T, ?> pattern;

    public CompiledCepRule(String naturalLanguageRule, CepPatternDescriptor descriptor, Pattern<T, ?> pattern) {
        this.naturalLanguageRule = naturalLanguageRule;
        this.descriptor = descriptor;
        this.pattern = pattern;
    }

    public String getNaturalLanguageRule() {
        return naturalLanguageRule;
    }

    public CepPatternDescriptor getDescriptor() {
        return descriptor;
    }

    public Pattern<T, ?> getPattern() {
        return pattern;
    }
}
