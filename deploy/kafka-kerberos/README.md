# Kafka Kerberos local test environment

This directory starts an isolated Kerberos-authenticated Kafka test stack. It does not start or modify any Studio service and does not replace the existing plaintext Kafka on port `9092`.

## Services and ports

| Service | Purpose | Host endpoint |
| --- | --- | --- |
| `kerberos-init` | Creates the realm, principals, and keytabs | One-shot container |
| `kerberos-kdc` | MIT Kerberos KDC | `127.0.0.1:10088` TCP/UDP |
| `kafka-storage-init` | Grants Kafka access to its isolated data volume | One-shot container |
| `kafka-kerberos` | Kafka 3.9.1 KRaft with SASL/GSSAPI | Host: `127.0.0.1:19092`; container: `kafka.kerberos.test:19094` |
| `kerberos-client` | Kafka CLI smoke-test client | Tools profile only |

Realm and principals:

```text
STUDIO.TEST
kafka/kafka.kerberos.test@STUDIO.TEST
kafka/cryptomator-vault@STUDIO.TEST
studio-client@STUDIO.TEST
```

## Start and verify

From this directory:

```bash
docker compose up -d --build kerberos-kdc kafka-kerberos
bash scripts/smoke-test.sh
docker compose ps
```

The smoke test creates `studio-kerberos-test`, produces and consumes a unique marker with the client keytab, and verifies that a request without SASL settings is rejected.

The existing `kafka-single` container and ports `9092-9093` are not touched.

## Endpoints and exported client files

Kafka advertises `127.0.0.1:19092` to host clients, so Windows Studio/Worker
processes do not require a custom hosts entry. Compose clients use the isolated
internal listener `kafka.kerberos.test:19094`.

The init container exports these untracked files:

```text
generated/studio-client.keytab
generated/kafka.keytab
```

Only `studio-client.keytab` is a client credential. `kafka.keytab` belongs to the broker and must not be supplied to application clients.

Use these datasource values when testing the DataAggregation Kafka plugin from a Windows JVM:

```text
bootstrap.servers=127.0.0.1:19092
kerberos=true
principal=studio-client@STUDIO.TEST
kerberosKeytabFilePath=<absolute deployment path>/generated/studio-client.keytab
krb5Conf=<absolute deployment path>/config/krb5-host.conf
kerberosDomain=STUDIO.TEST
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
```

Use forward slashes in Windows paths passed to JAAS, for example `C:/dev/.../studio-client.keytab`.

## Stop or reset

Stop the environment while retaining Kafka data and Kerberos identities:

```bash
docker compose --profile tools down
```

Delete all test Kafka data, Realm data, and generated keytabs:

```bash
docker compose --profile tools down --volumes
```

After deleting volumes, remove the files under `generated/` before starting again so the generated credentials cannot be confused with a previous Realm database.
