#include <WiFi.h>
#include <PubSubClient.h>
#include "DHT.h"

// ============================================================
// HARDWARE
// ============================================================

#define DHT_PIN       15
#define DHT_TYPE      DHT22

#define VIBRATION_PIN 34
#define LED_PIN       2

DHT dht(DHT_PIN, DHT_TYPE);

// ============================================================
// DEVICE
// ============================================================

const char* DEVICE_ID  = "ESP32_MACHINE_001";
const char* MACHINE_ID = "MACHINE_001";

// ============================================================
// WIFI - WOKWI
// ============================================================

const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASS = "";

// ============================================================
// MQTT
// ============================================================

const char* MQTT_SERVER = "host.wokwi.internal";
const int MQTT_PORT = 1883;

const char* MQTT_TOPIC =
    "factory/machine/MACHINE_001/telemetry";

const char* MQTT_CLIENT_ID =
    "ESP32_MACHINE_001";

// ============================================================
// THRESHOLDS
// ============================================================

const float TEMPERATURE_LIMIT = 90.0;
const float VIBRATION_LIMIT = 10.0;

// ============================================================
// TIMING
// ============================================================

const unsigned long READ_INTERVAL = 2000;

unsigned long lastRead = 0;

// ============================================================
// NETWORK OBJECTS
// ============================================================

WiFiClient espClient;

PubSubClient mqttClient(espClient);

// ============================================================
// WIFI
// ============================================================

void connectWiFi() {

  Serial.println();
  Serial.println("Connecting to WiFi...");

  WiFi.mode(WIFI_STA);

  WiFi.begin(WIFI_SSID, WIFI_PASS);

  int attempts = 0;

  while (WiFi.status() != WL_CONNECTED) {

    delay(500);

    Serial.print(".");

    attempts++;

    if (attempts >= 30) {

      Serial.println();
      Serial.println("ERROR: WiFi connection failed.");

      return;
    }
  }

  Serial.println();
  Serial.println("WiFi connected.");

  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());
}

// ============================================================
// MQTT
// ============================================================

void connectMQTT() {

  while (!mqttClient.connected()) {

    Serial.print("Connecting to MQTT... ");

    if (mqttClient.connect(MQTT_CLIENT_ID)) {

      Serial.println("CONNECTED");

      Serial.print("MQTT Broker: ");
      Serial.print(MQTT_SERVER);
      Serial.print(":");
      Serial.println(MQTT_PORT);

      Serial.print("MQTT Topic: ");
      Serial.println(MQTT_TOPIC);

    } else {

      Serial.print("FAILED, rc=");
      Serial.print(mqttClient.state());

      Serial.println(" - retrying in 2 seconds");

      delay(2000);
    }
  }
}

// ============================================================
// SETUP
// ============================================================

void setup() {

  Serial.begin(115200);

  delay(1000);

  Serial.println();
  Serial.println("========================================");
  Serial.println("       INDUSTRIAL IoT - MACHINE 001");
  Serial.println("========================================");

  pinMode(LED_PIN, OUTPUT);

  digitalWrite(LED_PIN, LOW);

  dht.begin();

  Serial.println("ESP32 initialized.");
  Serial.println("DHT22 initialized.");
  Serial.println("Potentiometer configured as vibration.");

  // WiFi
  connectWiFi();

  // MQTT
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);

  connectMQTT();

  Serial.println();
  Serial.println("System ready.");
  Serial.println();
}

// ============================================================
// LOOP
// ============================================================

void loop() {

  // ----------------------------------------------------------
  // MQTT CONNECTION
  // ----------------------------------------------------------

  if (!mqttClient.connected()) {

    connectMQTT();
  }

  mqttClient.loop();

  // ----------------------------------------------------------
  // SENSOR INTERVAL
  // ----------------------------------------------------------

  if (millis() - lastRead >= READ_INTERVAL) {

    lastRead = millis();

    // --------------------------------------------------------
    // DHT22
    // --------------------------------------------------------

    float temperature = dht.readTemperature();

    float humidity = dht.readHumidity();

    // --------------------------------------------------------
    // VIBRATION
    // --------------------------------------------------------

    int analogValue = analogRead(VIBRATION_PIN);

    float vibration =
        (analogValue / 4095.0) * 15.0;

    // --------------------------------------------------------
    // VALIDATION
    // --------------------------------------------------------

    if (isnan(temperature) || isnan(humidity)) {

      Serial.println("ERROR: DHT22 reading failed.");

      return;
    }

    // --------------------------------------------------------
    // ANOMALY
    // --------------------------------------------------------

    bool anomaly = false;

    if (temperature > TEMPERATURE_LIMIT) {

      anomaly = true;
    }

    if (vibration > VIBRATION_LIMIT) {

      anomaly = true;
    }

    // --------------------------------------------------------
    // LED
    // --------------------------------------------------------

    digitalWrite(
        LED_PIN,
        anomaly ? HIGH : LOW
    );

    // --------------------------------------------------------
    // STATUS
    // --------------------------------------------------------

    const char* status =
        anomaly ? "ANOMALY" : "NORMAL";

    // --------------------------------------------------------
    // JSON
    // --------------------------------------------------------

    char payload[512];

    snprintf(
        payload,
        sizeof(payload),

        "{"
        "\"device_id\":\"%s\","
        "\"machine_id\":\"%s\","
        "\"temperature\":%.2f,"
        "\"humidity\":%.2f,"
        "\"vibration\":%.2f,"
        "\"analog_value\":%d,"
        "\"status\":\"%s\","
        "\"alarm\":%s"
        "}",

        DEVICE_ID,
        MACHINE_ID,
        temperature,
        humidity,
        vibration,
        analogValue,
        status,
        anomaly ? "true" : "false"
    );

    // --------------------------------------------------------
    // SERIAL
    // --------------------------------------------------------

    Serial.println("----------------------------------------");

    Serial.print("Device ID   : ");
    Serial.println(DEVICE_ID);

    Serial.print("Machine ID  : ");
    Serial.println(MACHINE_ID);

    Serial.print("Temperature : ");
    Serial.print(temperature, 2);
    Serial.println(" °C");

    Serial.print("Humidity    : ");
    Serial.print(humidity, 2);
    Serial.println(" %");

    Serial.print("Vibration   : ");
    Serial.print(vibration, 2);
    Serial.println(" mm/s");

    Serial.print("Analog      : ");
    Serial.println(analogValue);

    Serial.print("Status      : ");
    Serial.println(status);

    Serial.print("Alarm       : ");
    Serial.println(anomaly ? "ON" : "OFF");

    Serial.print("MQTT JSON   : ");
    Serial.println(payload);

    // --------------------------------------------------------
    // MQTT PUBLISH
    // --------------------------------------------------------

    if (mqttClient.publish(MQTT_TOPIC, payload)) {

      Serial.println("MQTT Publish: SUCCESS");

    } else {

      Serial.println("MQTT Publish: FAILED");
    }

    Serial.println("----------------------------------------");
  }
}
