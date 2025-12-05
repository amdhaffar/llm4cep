package com.example.cep.engine;

import com.example.cep.config.KafkaConfig;
import com.example.cep.llm.CepPatternDescriptor;
import com.example.cep.llm.CompiledCepRule;
import com.example.cep.llm.MockLlmClient;
import com.example.cep.llm.RuleEngineService;
import com.example.cep.model.EnvironmentalEvent;
import com.example.cep.model.MedicalEvent;
import com.example.cep.model.RecommendationEvent;
import com.example.cep.model.UnifiedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Main Flink job wiring Kafka sources, LLM rule generation and CEP execution.
 */
public class FlinkCepJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCepJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ObjectMapper mapper = new ObjectMapper();

        // Kafka sources reading JSON strings
        KafkaSource<String> medicalSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.MEDICAL_TOPIC)
                .setGroupId("cep-medical-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> environmentalSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setTopics(KafkaConfig.ENVIRONMENTAL_TOPIC)
                .setGroupId("cep-env-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<MedicalEvent> medicalWatermarks = WatermarkStrategy
                .<MedicalEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<MedicalEvent>) (element, recordTimestamp) -> element.getTimestamp());

        WatermarkStrategy<EnvironmentalEvent> environmentalWatermarks = WatermarkStrategy
                .<EnvironmentalEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<EnvironmentalEvent>) (element, recordTimestamp) -> element.getTimestamp());

        DataStreamSource<String> medicalJson = env.fromSource(medicalSource, WatermarkStrategy.noWatermarks(), "medical-source");
        DataStreamSource<String> environmentalJson = env.fromSource(environmentalSource, WatermarkStrategy.noWatermarks(), "environmental-source");

        DataStream<MedicalEvent> medicalStream = medicalJson
                .map(value -> mapper.readValue(value, MedicalEvent.class))
                .assignTimestampsAndWatermarks(medicalWatermarks);

        DataStream<EnvironmentalEvent> environmentalStream = environmentalJson
                .map(value -> mapper.readValue(value, EnvironmentalEvent.class))
                .assignTimestampsAndWatermarks(environmentalWatermarks);

        KeyedStream<MedicalEvent, String> keyedMedical = medicalStream.keyBy(MedicalEvent::getPatientId);
        KeyedStream<UnifiedEvent, String> keyedUnified = medicalStream
                .map(event -> new UnifiedEvent(UnifiedEvent.Kind.MEDICAL, event, null))
                .union(environmentalStream.map(envEvent -> new UnifiedEvent(UnifiedEvent.Kind.ENVIRONMENTAL, null, envEvent)))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UnifiedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<UnifiedEvent>) (element, recordTimestamp) -> {
                            if (element.isMedical()) {
                                return element.getMedicalEvent().getTimestamp();
                            }
                            return element.getEnvironmentalEvent().getTimestamp();
                        }))
                .keyBy(UnifiedEvent::getLocationId);

        // Ask the LLM (mock) for rules and build CEP patterns
        RuleEngineService ruleEngineService = new RuleEngineService(new MockLlmClient());
        List<CompiledCepRule<?>> rules = ruleEngineService.getCompiledCepRules();

        // Kafka sink for recommendations (optional)
        KafkaSink<String> recommendationSink = KafkaSink.<String>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(KafkaConfig.RECOMMENDATION_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        for (CompiledCepRule<?> compiledRule : rules) {
            CepPatternDescriptor descriptor = compiledRule.getDescriptor();
            switch (descriptor.getTargetStreamType()) {
                case MEDICAL:
                    applyMedicalRule(keyedMedical, (CompiledCepRule<MedicalEvent>) compiledRule, mapper, recommendationSink, params);
                    break;
                case COMBINED:
                    applyCombinedRule(keyedUnified, (CompiledCepRule<UnifiedEvent>) compiledRule, mapper, recommendationSink, params);
                    break;
                default:
                    LOG.warn("Skipping unsupported target stream: {}", descriptor.getTargetStreamType());
            }
        }

        env.execute("LLM generated CEP demo");
    }

    private static void applyMedicalRule(KeyedStream<MedicalEvent, String> stream, CompiledCepRule<MedicalEvent> compiledRule,
                                         ObjectMapper mapper, KafkaSink<String> sink, ParameterTool params) {
        PatternStream<MedicalEvent> patternStream = CEP.pattern(stream, compiledRule.getPattern());
        DataStream<RecommendationEvent> recommendations = patternStream.select(new RecommendationFromMedical(compiledRule));
        recommendations.print();
        if (params.getBoolean("sinkKafka", true)) {
            recommendations.map(event -> mapper.writeValueAsString(event)).sinkTo(sink);
        }
    }

    private static void applyCombinedRule(KeyedStream<UnifiedEvent, String> stream, CompiledCepRule<UnifiedEvent> compiledRule,
                                          ObjectMapper mapper, KafkaSink<String> sink, ParameterTool params) {
        PatternStream<UnifiedEvent> patternStream = CEP.pattern(stream, compiledRule.getPattern());
        DataStream<RecommendationEvent> recommendations = patternStream.select(new RecommendationFromUnified(compiledRule));
        recommendations.print();
        if (params.getBoolean("sinkKafka", true)) {
            recommendations.map(event -> mapper.writeValueAsString(event)).sinkTo(sink);
        }
    }

    private static class RecommendationFromMedical implements PatternSelectFunction<MedicalEvent, RecommendationEvent> {
        private final CompiledCepRule<MedicalEvent> compiledRule;

        private RecommendationFromMedical(CompiledCepRule<MedicalEvent> compiledRule) {
            this.compiledRule = compiledRule;
        }

        @Override
        public RecommendationEvent select(Map<String, List<MedicalEvent>> pattern) {
            MedicalEvent first = pattern.values().stream().findFirst().flatMap(list -> list.stream().findFirst()).orElse(null);
            String patientId = first != null ? first.getPatientId() : "unknown";
            String locationId = first != null ? first.getLocationId() : "unknown";
            return new RecommendationEvent(
                    patientId,
                    locationId,
                    System.currentTimeMillis(),
                    compiledRule.getDescriptor().getRecommendationType(),
                    "Détecté via règle: " + compiledRule.getNaturalLanguageRule()
            );
        }
    }

    private static class RecommendationFromUnified implements PatternSelectFunction<UnifiedEvent, RecommendationEvent> {
        private final CompiledCepRule<UnifiedEvent> compiledRule;

        private RecommendationFromUnified(CompiledCepRule<UnifiedEvent> compiledRule) {
            this.compiledRule = compiledRule;
        }

        @Override
        public RecommendationEvent select(Map<String, List<UnifiedEvent>> pattern) {
            UnifiedEvent any = pattern.values().stream().findFirst().flatMap(list -> list.stream().findFirst()).orElse(null);
            String patientId = any != null ? any.getPatientId() : "unknown";
            String locationId = any != null ? any.getLocationId() : "unknown";
            return new RecommendationEvent(
                    patientId,
                    locationId,
                    System.currentTimeMillis(),
                    compiledRule.getDescriptor().getRecommendationType(),
                    "Détecté via règle combinée: " + compiledRule.getNaturalLanguageRule()
            );
        }
    }
}
