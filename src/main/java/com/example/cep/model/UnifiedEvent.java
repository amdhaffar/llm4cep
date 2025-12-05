package com.example.cep.model;

import java.io.Serializable;

/**
 * Wrapper to merge medical and environmental events into a single stream for combined CEP rules.
 */
public class UnifiedEvent implements Serializable {

    public enum Kind {
        MEDICAL,
        ENVIRONMENTAL
    }

    private Kind kind;
    private MedicalEvent medicalEvent;
    private EnvironmentalEvent environmentalEvent;

    public UnifiedEvent() {
    }

    public UnifiedEvent(Kind kind, MedicalEvent medicalEvent, EnvironmentalEvent environmentalEvent) {
        this.kind = kind;
        this.medicalEvent = medicalEvent;
        this.environmentalEvent = environmentalEvent;
    }

    public Kind getKind() {
        return kind;
    }

    public MedicalEvent getMedicalEvent() {
        return medicalEvent;
    }

    public EnvironmentalEvent getEnvironmentalEvent() {
        return environmentalEvent;
    }

    public boolean isMedical() {
        return Kind.MEDICAL.equals(kind) && medicalEvent != null;
    }

    public boolean isEnvironmental() {
        return Kind.ENVIRONMENTAL.equals(kind) && environmentalEvent != null;
    }

    public String getLocationId() {
        if (isMedical()) {
            return medicalEvent.getLocationId();
        }
        if (isEnvironmental()) {
            return environmentalEvent.getLocationId();
        }
        return null;
    }

    public String getPatientId() {
        if (isMedical()) {
            return medicalEvent.getPatientId();
        }
        return null;
    }
}
