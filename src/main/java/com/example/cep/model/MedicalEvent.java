package com.example.cep.model;

import java.io.Serializable;

/**
 * Represents a medical vital sign measurement for a patient.
 */
public class MedicalEvent implements Serializable {

    private String patientId;
    private String locationId;
    private long timestamp;
    private double heartRate;
    private double systolicBloodPressure;
    private double diastolicBloodPressure;
    private double bloodOxygenLevel;

    public MedicalEvent() {
    }

    public MedicalEvent(String patientId, String locationId, long timestamp, double heartRate,
                        double systolicBloodPressure, double diastolicBloodPressure, double bloodOxygenLevel) {
        this.patientId = patientId;
        this.locationId = locationId;
        this.timestamp = timestamp;
        this.heartRate = heartRate;
        this.systolicBloodPressure = systolicBloodPressure;
        this.diastolicBloodPressure = diastolicBloodPressure;
        this.bloodOxygenLevel = bloodOxygenLevel;
    }

    public String getPatientId() {
        return patientId;
    }

    public String getLocationId() {
        return locationId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getHeartRate() {
        return heartRate;
    }

    public double getSystolicBloodPressure() {
        return systolicBloodPressure;
    }

    public double getDiastolicBloodPressure() {
        return diastolicBloodPressure;
    }

    public double getBloodOxygenLevel() {
        return bloodOxygenLevel;
    }

    @Override
    public String toString() {
        return "MedicalEvent{" +
                "patientId='" + patientId + '\'' +
                ", locationId='" + locationId + '\'' +
                ", timestamp=" + timestamp +
                ", heartRate=" + heartRate +
                ", systolicBloodPressure=" + systolicBloodPressure +
                ", diastolicBloodPressure=" + diastolicBloodPressure +
                ", bloodOxygenLevel=" + bloodOxygenLevel +
                '}';
    }
}
