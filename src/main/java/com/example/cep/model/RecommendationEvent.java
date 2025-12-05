package com.example.cep.model;

import java.io.Serializable;

/**
 * Output event produced by CEP rules.
 */
public class RecommendationEvent implements Serializable {

    private String patientId;
    private String locationId;
    private long detectedAt;
    private String recommendationType;
    private String reason;

    public RecommendationEvent() {
    }

    public RecommendationEvent(String patientId, String locationId, long detectedAt, String recommendationType, String reason) {
        this.patientId = patientId;
        this.locationId = locationId;
        this.detectedAt = detectedAt;
        this.recommendationType = recommendationType;
        this.reason = reason;
    }

    public String getPatientId() {
        return patientId;
    }

    public String getLocationId() {
        return locationId;
    }

    public long getDetectedAt() {
        return detectedAt;
    }

    public String getRecommendationType() {
        return recommendationType;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "RecommendationEvent{" +
                "patientId='" + patientId + '\'' +
                ", locationId='" + locationId + '\'' +
                ", detectedAt=" + detectedAt +
                ", recommendationType='" + recommendationType + '\'' +
                ", reason='" + reason + '\'' +
                '}';
    }
}
