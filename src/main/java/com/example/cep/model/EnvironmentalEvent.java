package com.example.cep.model;

import java.io.Serializable;

/**
 * Represents an environmental measurement for a given location.
 */
public class EnvironmentalEvent implements Serializable {

    private String locationId;
    private long timestamp;
    private double airQualityIndex;
    private double temperature;
    private double humidity;
    private double pm25;

    public EnvironmentalEvent() {
    }

    public EnvironmentalEvent(String locationId, long timestamp, double airQualityIndex, double temperature,
                              double humidity, double pm25) {
        this.locationId = locationId;
        this.timestamp = timestamp;
        this.airQualityIndex = airQualityIndex;
        this.temperature = temperature;
        this.humidity = humidity;
        this.pm25 = pm25;
    }

    public String getLocationId() {
        return locationId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getAirQualityIndex() {
        return airQualityIndex;
    }

    public double getTemperature() {
        return temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public double getPm25() {
        return pm25;
    }

    @Override
    public String toString() {
        return "EnvironmentalEvent{" +
                "locationId='" + locationId + '\'' +
                ", timestamp=" + timestamp +
                ", airQualityIndex=" + airQualityIndex +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                ", pm25=" + pm25 +
                '}';
    }
}
