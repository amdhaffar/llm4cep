package com.example.cep.llm;

import com.example.cep.model.MedicalEvent;
import com.example.cep.model.UnifiedEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Service responsible for asking the LLM for rules and compiling them into Flink CEP patterns.
 */
public class RuleEngineService {

    private static final Logger LOG = LoggerFactory.getLogger(RuleEngineService.class);

    private final LlmClient llmClient;
    private final List<CompiledCepRule<?>> compiledRules = new ArrayList<>();

    public RuleEngineService(LlmClient llmClient) {
        this.llmClient = llmClient;
        loadRules();
    }

    private void loadRules() {
        List<String> naturalRules = llmClient.generateNaturalLanguageRules();
        for (String naturalRule : naturalRules) {
            LOG.info("Natural language rule from LLM: {}", naturalRule);
            CepPatternDescriptor descriptor = llmClient.generateCepPatternDescriptor(naturalRule);
            Pattern<?, ?> pattern = buildPattern(descriptor);
            compiledRules.add(new CompiledCepRule<>(naturalRule, descriptor, pattern));
        }
    }

    private Pattern<?, ?> buildPattern(CepPatternDescriptor descriptor) {
        Duration window = descriptor.getWindow();
        switch (descriptor.getRuleId()) {
            case "RULE_A":
                return buildRuleA(window, descriptor.getHeartRateThreshold(), descriptor.getBloodOxygenThreshold());
            case "RULE_B":
                return buildRuleB(window, descriptor.getAqiThreshold(), descriptor.getHeartRateThreshold());
            case "RULE_C":
                return buildRuleC(window, descriptor.getSystolicThreshold());
            default:
                return buildGenericMedical(window, descriptor.getHeartRateThreshold(), descriptor.getBloodOxygenThreshold());
        }
    }

    private Pattern<MedicalEvent, ?> buildRuleA(Duration window, double hrThreshold, double spo2Threshold) {
        return Pattern.<MedicalEvent>begin("highHeartRate")
                .where(new SimpleCondition<MedicalEvent>() {
                    @Override
                    public boolean filter(MedicalEvent event) {
                        return event.getHeartRate() > hrThreshold;
                    }
                })
                .next("lowOxygen")
                .where(new SimpleCondition<MedicalEvent>() {
                    @Override
                    public boolean filter(MedicalEvent event) {
                        return event.getBloodOxygenLevel() < spo2Threshold;
                    }
                })
                .within(Time.minutes(window.toMinutes()));
    }

    private Pattern<UnifiedEvent, ?> buildRuleB(Duration window, double aqiThreshold, double heartRateThreshold) {
        return Pattern.<UnifiedEvent>begin("pollutedArea")
                .where(new SimpleCondition<UnifiedEvent>() {
                    @Override
                    public boolean filter(UnifiedEvent event) {
                        return event.isEnvironmental() && event.getEnvironmentalEvent().getAirQualityIndex() > aqiThreshold;
                    }
                })
                .next("stressedPatient")
                .where(new SimpleCondition<UnifiedEvent>() {
                    @Override
                    public boolean filter(UnifiedEvent event) {
                        return event.isMedical() && event.getMedicalEvent().getHeartRate() > heartRateThreshold;
                    }
                })
                .within(Time.minutes(window.toMinutes()));
    }

    private Pattern<MedicalEvent, ?> buildRuleC(Duration window, double systolicThreshold) {
        return Pattern.<MedicalEvent>begin("firstHighBP")
                .where(new SimpleCondition<MedicalEvent>() {
                    @Override
                    public boolean filter(MedicalEvent event) {
                        return event.getSystolicBloodPressure() > systolicThreshold;
                    }
                })
                .next("secondHighBP")
                .where(new SimpleCondition<MedicalEvent>() {
                    @Override
                    public boolean filter(MedicalEvent event) {
                        return event.getSystolicBloodPressure() > systolicThreshold;
                    }
                })
                .within(Time.minutes(window.toMinutes()));
    }

    private Pattern<MedicalEvent, ?> buildGenericMedical(Duration window, double hrThreshold, double spo2Threshold) {
        return Pattern.<MedicalEvent>begin("genericMedical")
                .where(new SimpleCondition<MedicalEvent>() {
                    @Override
                    public boolean filter(MedicalEvent event) {
                        return event.getHeartRate() > hrThreshold || event.getBloodOxygenLevel() < spo2Threshold;
                    }
                })
                .within(Time.minutes(window.toMinutes()));
    }

    public List<CompiledCepRule<?>> getCompiledCepRules() {
        return compiledRules;
    }
}
