package com.example.cep.llm;

import java.time.Duration;

/**
 * Simplified structured description of a CEP rule coming from the LLM.
 */
public class CepPatternDescriptor {

    public enum TargetStreamType {
        MEDICAL,
        ENVIRONMENTAL,
        COMBINED
    }

    private String ruleId;
    private String naturalLanguageRule;
    private TargetStreamType targetStreamType;
    private Duration window;
    private double heartRateThreshold;
    private double bloodOxygenThreshold;
    private double systolicThreshold;
    private double aqiThreshold;
    private String recommendationType;

    public CepPatternDescriptor(String ruleId, String naturalLanguageRule, TargetStreamType targetStreamType,
                                Duration window, double heartRateThreshold, double bloodOxygenThreshold,
                                double systolicThreshold, double aqiThreshold, String recommendationType) {
        this.ruleId = ruleId;
        this.naturalLanguageRule = naturalLanguageRule;
        this.targetStreamType = targetStreamType;
        this.window = window;
        this.heartRateThreshold = heartRateThreshold;
        this.bloodOxygenThreshold = bloodOxygenThreshold;
        this.systolicThreshold = systolicThreshold;
        this.aqiThreshold = aqiThreshold;
        this.recommendationType = recommendationType;
    }

    public String getRuleId() {
        return ruleId;
    }

    public String getNaturalLanguageRule() {
        return naturalLanguageRule;
    }

    public TargetStreamType getTargetStreamType() {
        return targetStreamType;
    }

    public Duration getWindow() {
        return window;
    }

    public double getHeartRateThreshold() {
        return heartRateThreshold;
    }

    public double getBloodOxygenThreshold() {
        return bloodOxygenThreshold;
    }

    public double getSystolicThreshold() {
        return systolicThreshold;
    }

    public double getAqiThreshold() {
        return aqiThreshold;
    }

    public String getRecommendationType() {
        return recommendationType;
    }
}
