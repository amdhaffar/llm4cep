package com.example.cep.llm;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Mock implementation that simulates LLM responses.
 * Replace this class with an HTTP client that calls a real LLM API (OpenAI, etc.).
 */
public class MockLlmClient implements LlmClient {

    @Override
    public List<String> generateNaturalLanguageRules() {
        // In a real implementation, the prompt would be sent to the LLM and the natural language
        // rules would be returned by the model.
        return Arrays.asList(
                "Si la fréquence cardiaque d’un patient dépasse 120 bpm et que la SpO2 descend sous 92% dans une fenêtre de 5 minutes, alors générer une recommandation de consultation urgente.",
                "Si la qualité de l’air (AQI) est supérieure à 150 et qu’un patient a une fréquence cardiaque supérieure à 100 bpm dans la même zone pendant 10 minutes, recommander d’éviter les activités à l’extérieur.",
                "Si la pression artérielle systolique dépasse 160 sur au moins deux mesures consécutives en 30 minutes, générer une alerte médicale."
        );
    }

    @Override
    public CepPatternDescriptor generateCepPatternDescriptor(String naturalLanguageRule) {
        // A real implementation would call the LLM again with a prompt like "Translate this rule to CEP".
        // Here we map known demo rules to descriptors manually.
        if (naturalLanguageRule.contains("SpO2") || naturalLanguageRule.contains("bpm")) {
            return new CepPatternDescriptor(
                    "RULE_A",
                    naturalLanguageRule,
                    CepPatternDescriptor.TargetStreamType.MEDICAL,
                    Duration.ofMinutes(5),
                    120,
                    92,
                    0,
                    0,
                    "CONSULTATION_URGENTE"
            );
        }
        if (naturalLanguageRule.contains("qualité de l’air") || naturalLanguageRule.contains("AQI")) {
            return new CepPatternDescriptor(
                    "RULE_B",
                    naturalLanguageRule,
                    CepPatternDescriptor.TargetStreamType.COMBINED,
                    Duration.ofMinutes(10),
                    100,
                    0,
                    0,
                    150,
                    "EVITER_EXTERIEUR"
            );
        }
        if (naturalLanguageRule.contains("pression artérielle") || naturalLanguageRule.contains("systolique")) {
            return new CepPatternDescriptor(
                    "RULE_C",
                    naturalLanguageRule,
                    CepPatternDescriptor.TargetStreamType.MEDICAL,
                    Duration.ofMinutes(30),
                    0,
                    0,
                    160,
                    0,
                    "ALERTE_TENSION"
            );
        }
        // Default fallback rule for unknown text
        return new CepPatternDescriptor(
                "RULE_UNKNOWN",
                naturalLanguageRule,
                CepPatternDescriptor.TargetStreamType.MEDICAL,
                Duration.ofMinutes(15),
                110,
                92,
                150,
                150,
                "RECOMMANDATION_GENERIQUE"
        );
    }
}
