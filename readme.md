# LLM4CEP Demo

Projet démonstrateur Flink CEP + Kafka + LLM (mock) pour générer et appliquer des règles métier sur des flux médicaux et environnementaux.

## Démarrage rapide

1. **Démarrer Kafka** (Zookeeper/Kafka en local sur `localhost:9092`) et créer les topics :
   ```bash
   kafka-topics.sh --create --topic medical-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics.sh --create --topic environmental-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics.sh --create --topic recommendations --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
2. **Lancer le générateur Kafka** pour produire en continu des événements JSON :
   ```bash
   mvn -q -DskipTests package
   java -cp target/llm4cep-1.0.0.jar com.example.cep.generator.KafkaDataGenerator
   ```
3. **Démarrer le job Flink** qui consomme Kafka, demande les règles à l’LLM mock et applique les patterns CEP :
   ```bash
   java -cp target/llm4cep-1.0.0.jar com.example.cep.engine.FlinkCepJob
   ```
   Ajoutez `--sinkKafka false` si vous souhaitez uniquement afficher les recommandations dans les logs.
4. **Observer les recommandations** dans la console ou en consommant le topic `recommendations` :
   ```bash
   kafka-console-consumer.sh --topic recommendations --bootstrap-server localhost:9092
   ```

## LLM mock et extension

* `MockLlmClient` simule les réponses d’un LLM : il renvoie des règles en texte libre puis les convertit en descripteurs CEP (`CepPatternDescriptor`).
* Pour connecter un vrai LLM (OpenAI / API REST), remplacez l’implémentation par un client HTTP dans `generateNaturalLanguageRules()` et `generateCepPatternDescriptor()` (clé API, URL, prompt...). Le reste du code reste identique.

## Règles couvertes

* Règle A : tachycardie (>120 bpm) suivie d’une SpO2 < 92 % dans 5 minutes → **consultation urgente**.
* Règle B : AQI > 150 puis fréquence cardiaque > 100 bpm sur la même zone en 10 minutes → **éviter les activités à l’extérieur**.
* Règle C : pression systolique > 160 sur deux mesures en 30 minutes → **alerte tension**.

## Notes

* Code compilable en Java 17 / Maven, dépendances Flink 1.17.2.
* Les schémas de sérialisation utilisent JSON (Jackson) et les logs passent par SLF4J.
