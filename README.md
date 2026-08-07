# Appian MQTT Connected System Plug-in

An Appian Suite plug-in that provides native MQTT 3.1.1 connectivity for publishing and subscribing to messages. It uses the Eclipse Paho asynchronous MQTT client and exposes MQTT operations through Appian Connected Systems, Integration Templates, Process Modeler Smart Services, and an expression function.

The plug-in is intended for IoT and edge-integration workflows where Appian needs to communicate directly with an MQTT broker instead of relying on an HTTP translation service such as a Web API, Lambda function, or Node-RED flow.

## Capabilities

- Connect to MQTT 3.1.1 brokers using `tcp://` URLs.
- Authenticate with optional username and password credentials.
- Publish text or JSON payloads with MQTT QoS 0, 1, or 2.
- Set the MQTT retained-message flag through the Integration Template and Publish Smart Service.
- Subscribe in either of two modes:
  - **ONE_SHOT** — collect messages until a maximum count is reached or a timeout expires, then return the messages as JSON.
  - **PERSISTENT** — run a background listener that filters and throttles inbound messages and starts an Appian process for each accepted message.
- Reuse broker connections through a JVM-wide connection manager.
- Bound connection and worker-pool usage and evict idle connections automatically.
- Buffer persistent inbound messages in a bounded queue so an MQTT callback does not block while Appian processes events.

## Appian objects provided

The plug-in manifest (`src/main/resources/appian-plugin.xml`) registers the following objects:

| Object | Appian location | Purpose |
|---|---|---|
| `mqttConnectedSystem` | Connected Systems | Stores reusable broker connection properties. |
| `mqttPublishIntegration` | Integration Designer | Publishes a message using an MQTT Connected System. |
| `mqttPublishSmartService` | Process Modeler → Integration Services → MQTT | Publishes a message when a process node runs. |
| `mqttSubscribeSmartService` | Process Modeler → Integration Services → MQTT | Collects messages once or starts a persistent listener. |
| `mqttPublishFunction` | Expression Editor, category `mqttFunctions` | Publishes a message and returns a JSON result. |

The plug-in key is `com.example.appian.mqtt`, and the current manifest requires Appian 26.0 or newer. The Maven project currently targets Appian SDK `26.7`.

## Architecture

All publish and subscribe components use `CentralConnectionManager` to obtain a shared `MqttAsyncClient` connection. Connections are keyed by `brokerUrl::clientId` and wrapped by `SocketHolder`, which tracks activity and closes clients safely.

Current runtime defaults and limits are:

| Setting | Value | Details |
|---|---:|---|
| MQTT client | Paho `1.2.5` | MQTT 3.1.1 client. |
| Connection timeout | 10 seconds | Used by the publish and subscribe components. |
| Automatic reconnect | Enabled | Configured on each Paho client. |
| Maximum active connections | 20 | New connections fail when the pool is full. |
| Worker threads | 5 | Shared scheduled/background worker pool. |
| Idle eviction interval | 30 seconds | The manager checks for idle connections at this interval. |
| Idle timeout | 60 seconds | Connections unused longer than this are closed. |
| Paho max in-flight messages | 10 | Set when a connection is created. |
| Persistent listener queue | 1,000 messages | Default bounded queue capacity. |
| Persistent listener rate | 50 messages/second | Default maximum accepted rate. |

The connection manager uses clean sessions for the current publish and subscribe call sites. The Connected System UI exposes `cleanSession` and `keepAlive`, but those values are not currently passed into `CentralConnectionManager` by the existing publish/subscribe implementations. Treat these properties as reserved for a future configuration improvement rather than assuming they change the current runtime behavior.

## Prerequisites

| Requirement | Version or source |
|---|---|
| Java Development Kit | Java 17. Appian's bundled JVM is also Java 17. |
| Maven | Maven 3.9+ if using a system installation; the repository includes `mvnw`/`mvnw.cmd`. |
| Appian Plug-in SDK | SDK `26.7`, or a version compatible with the target Appian environment. |
| MQTT broker | An accessible MQTT 3.1.1 broker reachable over `tcp://`. |
| Appian environment | Appian 26.0 or newer according to the plug-in manifest. |

### Select Java 17 on macOS

```bash
/usr/libexec/java_home -V
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
java -version
```

On Windows, set `JAVA_HOME` to the Java 17 installation before running `mvnw.cmd`.

## Configure the Appian SDK dependency

The Appian Plug-in SDK is not available from Maven Central. The Maven build expects the SDK JAR at:

```text
libs/appian-plug-in-sdk.jar
```

If the file is not already present, copy it from an Appian installation or obtain the matching SDK from your Appian administrator:

```bash
cp /path/to/appian-plug-in-sdk.jar libs/appian-plug-in-sdk.jar
```

The default Maven configuration references that local file with `system` scope. The SDK is not bundled into the final plug-in JAR because Appian supplies it at runtime.

For CI/CD or a shared local Maven repository, install the SDK JAR once:

```bash
./mvnw install:install-file \
  -Dfile=libs/appian-plug-in-sdk.jar \
  -DgroupId=com.appian \
  -DartifactId=appian-plug-in-sdk \
  -Dversion=26.7 \
  -Dpackaging=jar
```

Then use the `local-m2` profile when building:

```bash
./mvnw clean package -Plocal-m2
```

Update both the installed version and `<appian.sdk.version>` in `pom.xml` when targeting a different SDK version.

## Build the plug-in

From the repository root:

```bash
# Compile and run the test phase
./mvnw clean test

# Build the deployable shaded JAR
./mvnw clean package
```

On Windows:

```bat
mvnw.cmd clean package
```

The package phase uses the Maven Shade Plugin to include the runtime third-party dependencies, including Paho and Jackson, while leaving the Appian SDK provided by Appian. The expected output is:

```text
target/appian-mqtt-plugin-1.0.0.jar
```

Verify that the required manifest is at the root of the JAR:

```bash
jar tf target/appian-mqtt-plugin-1.0.0.jar | grep appian-plugin.xml
```

The command should print `appian-plugin.xml` rather than a path nested under a Java package.

## Deploy to Appian

1. Build the JAR using the steps above.
2. Copy it to the Appian plug-ins directory:

   ```bash
   cp target/appian-mqtt-plugin-1.0.0.jar <APPIAN_HOME>/_admin/plugins/
   ```

3. Allow Appian's plug-in polling process to detect the file. A server restart is normally not required for a plug-in deployment, but the exact detection time depends on the environment's plug-in polling configuration.
4. Check the Appian suite log for plug-in loading or class-loading errors:

   ```text
   <APPIAN_HOME>/logs/appian-suite.log
   ```

5. In Appian Designer, confirm that the MQTT Connected System, MQTT integration, and MQTT Smart Services are available.

Use a broker URL that the Appian application server can reach. A broker reachable from a developer workstation is not necessarily reachable from the Appian server network.

## Configure an MQTT Connected System

Create a Connected System using the MQTT template and configure:

| Property | Required | Description | Current default |
|---|---|---|---|
| **Broker URL** | Yes | MQTT broker address, for example `tcp://broker.example.com:1883`. | — |
| **Client ID** | Yes | Unique MQTT client identifier. Use a distinct ID where the broker requires unique clients. | — |
| **Username** | No | Broker authentication username. | — |
| **Password** | No | Broker authentication password. Appian stores this as an encrypted text property. | — |
| **Keep Alive** | No | UI property described as seconds. | `60` |
| **Clean Session** | No | UI property controlling session behavior. | `true` |

The Connected System is used by the `mqttPublishIntegration` Integration Template. The current Smart Services accept broker properties as their own inputs rather than reading this Connected System automatically.

## Publish messages

### Integration Template

Use **MQTT Publish** in Integration Designer and select an MQTT Connected System. Configure:

- **MQTT Topic** — required topic name.
- **Payload (JSON/Text)** — required text or JSON payload; multiline input is supported.
- **QoS Level** — `0`, `1`, or `2`; defaults to `0`.
- **Retain Message** — optional Boolean; defaults to `false`.

The integration returns a diagnostic result containing the topic, QoS, retained flag, status, published topic, and payload size. On failure, it returns an `MQTT_PUBLISH_FAILED` integration error and an error message.

### Publish Smart Service

Add **MQTT Publish** from the MQTT section of the Process Modeler palette. Its inputs are:

- `brokerUrl` — required, for example `tcp://broker.example.com:1883`.
- `clientId` — required.
- `username` and `password` — optional broker credentials.
- `topic` — required MQTT topic.
- `payload` — required message text or JSON.
- `qos` — optional QoS `0`, `1`, or `2`; defaults to `0`.
- `retained` — optional retained flag; defaults to `false`.

Outputs are:

- `success` — `true` after a successful publish.
- `errorMessage` — error text when the publish fails.
- `messageId` — generated identifier containing the connection key, topic, and publish timestamp on success.

The Smart Service catches publish errors and exposes them through outputs instead of rethrowing them, allowing the process model to branch on `success`.

### Expression function

The `mqttPublish` expression function accepts a broker URL, client ID, topic, payload, and QoS:

```appian
mqttPublish(
  "tcp://broker.example.com:1883",
  "appian-publisher-01",
  "sensors/temperature",
  "{\"temperature\":25.4,\"unit\":\"C\"}",
  1
)
```

It returns a JSON string. A successful response has this shape:

```json
{"status":"SUCCESS","topic":"sensors/temperature","messageId":"..."}
```

An error response has this shape:

```json
{"status":"ERROR","errorMessage":"..."}
```

The expression function currently does not accept username or password parameters and publishes without broker credentials. It also has a side effect, so avoid placing it in expressions that Appian evaluates repeatedly or in interfaces where repeated evaluation could publish duplicate messages. Prefer the Integration Template or Publish Smart Service when credentials, retained messages, or explicit process control are needed.

## Subscribe to messages

Add **MQTT Subscribe** from the MQTT section of the Process Modeler palette. Common inputs are:

- `brokerUrl` — required broker URL.
- `clientId` — required MQTT client ID.
- `username` and `password` — optional broker credentials.
- `topic` — required topic or topic filter.
- `qos` — optional subscription QoS; defaults to `0`.
- `mode` — required `ONE_SHOT` or `PERSISTENT`.

### ONE_SHOT mode

`ONE_SHOT` subscribes, collects messages, unsubscribes, and returns. Optional inputs:

- `maxMessages` — maximum messages to collect; defaults to `10`.
- `timeoutMs` — wait time in milliseconds; defaults to `5000`.

The outputs are:

- `success` — `true` when the subscription operation completes.
- `errorMessage` — error text if the operation fails.
- `collectedMessages` — JSON array of collected messages.

Each collected message contains fields similar to:

```json
[
  {
    "topic": "sensors/temperature",
    "payload": "{\"temperature\":25.4}",
    "qos": 0,
    "retained": false,
    "timestamp": 1720000000000
  }
]
```

A successful timeout with no messages still completes successfully and returns an empty JSON array.

### PERSISTENT mode

`PERSISTENT` starts a background listener. It requires:

- `processModelId` — ID of the Appian process model to start for each accepted MQTT message.
- `serviceAccountUsername` — Appian service account used to create the process execution context.

Optional controls are:

- `maxMessagesPerSecond` — inbound rate limit; defaults to `50`. Values less than or equal to `0` are also replaced by the current default of `50` by the Smart Service.
- `queueCapacity` — bounded inbound queue size; defaults to `1000`.
- `jsonFilterExpression` — numeric JSON filter such as `temperature > 80`.

Each accepted message starts the configured process model with these process variables:

| Process variable | Value |
|---|---|
| `mqttTopic` | Received MQTT topic. |
| `mqttPayload` | UTF-8 payload text. |
| `mqttQos` | Received QoS as a number. |
| `mqttTimestamp` | Message receipt timestamp in milliseconds. |

The Smart Service returns a generated `listenerId` that identifies the active listener. The listener registry is held in the JVM, and `stopListener(listenerId)` performs an idempotent unsubscribe and cleanup in Java. There is currently no separate Appian-visible stop Smart Service, so plan listener lifecycle management accordingly.

The JSON filter supports the format `field operator value` for numeric fields and these operators: `>`, `>=`, `<`, `<=`, `==`, and `!=`. For example, with payload `{"temperature":85}`, the filter `temperature > 80` accepts the message. Messages can be dropped because of the filter, rate limit, or full queue; the listener handle exposes accepted, dropped, and queued counts to Java callers.

## Broker and security limitations

- The implementation currently targets MQTT 3.1.1 through Eclipse Paho.
- The documented and implemented connection form is `tcp://host:port`; TLS/SSL (`ssl://`) is not implemented in this version.
- No broker URL or QoS validation is performed consistently across all Appian objects; configure valid values in Appian.
- Credentials should be supplied through protected Appian properties or process configuration. Do not hard-code passwords in expressions or source files.
- The Appian server must have network access to the broker and any required firewall, DNS, and broker ACL rules must be configured outside this project.

## Project structure

```text
appian-mqtt-plugin/
├── pom.xml
├── mvnw / mvnw.cmd
├── libs/
│   └── appian-plug-in-sdk.jar
├── src/main/
│   ├── java/com/example/appian/mqtt/
│   │   ├── core/
│   │   │   ├── AppianProcessLauncher.java
│   │   │   ├── CentralConnectionManager.java
│   │   │   ├── MqttInboundThrottler.java
│   │   │   └── SocketHolder.java
│   │   ├── functions/
│   │   │   └── MqttPublishFunction.java
│   │   ├── smartservices/
│   │   │   ├── MqttPublishSmartService.java
│   │   │   └── MqttSubscribeSmartService.java
│   └── resources/appian-plugin.xml
├── src/main/java/com/appian/...        # Simplified Connected System SDK types
├── src/main/java/com/appiancorp/...    # Appian process/service compatibility types
└── target/                              # Generated by Maven; not source-controlled
```

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| Maven cannot resolve `appian-plug-in-sdk` | The SDK JAR is missing or the version is wrong. | Put the JAR in `libs/` or install it and use `-Plocal-m2`. |
| `UnsupportedClassVersionError` in Appian | The plug-in was compiled with a newer JDK. | Build with Java 17. |
| Plug-in is not listed in Appian | Manifest missing, malformed, or JAR not detected. | Confirm `appian-plugin.xml` is at the JAR root and inspect `appian-suite.log`. |
| Connection timeout or refused connection | Appian cannot reach the broker or the URL is invalid. | Test DNS/firewall access from the Appian server and use a valid `tcp://` URL. |
| Authentication failure | Broker credentials or ACLs are incorrect. | Verify username, password, and topic permissions. |
| Persistent messages are dropped | Rate limit or bounded queue capacity was exceeded. | Increase `maxMessagesPerSecond`/`queueCapacity` carefully and monitor listener counters. |
| Persistent listener cannot start | `processModelId`, `serviceAccountUsername`, or the Appian process execution service is unavailable. | Supply both required values and verify the service account can start the process model. |

## Development notes

- Compile with Java 17 to match the target Appian JVM.
- Keep the Appian SDK provided by Appian at runtime; do not package it into the shaded JAR.
- Keep `appian-plugin.xml` in `src/main/resources` so Maven places it at the JAR root.
- Change the plug-in key and vendor metadata in `appian-plugin.xml` before distributing this plug-in outside the example organization.
- Update `appian.sdk.version` and the SDK installation command together when changing Appian versions.
