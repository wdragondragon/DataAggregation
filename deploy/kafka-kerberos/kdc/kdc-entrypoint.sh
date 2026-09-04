#!/usr/bin/env bash
set -euo pipefail

realm="${KRB5_REALM:-STUDIO.TEST}"
master_password="${KDC_MASTER_PASSWORD:-studio-kdc-master-change-me}"
broker_principal="${KAFKA_BROKER_PRINCIPAL:-kafka/kafka.kerberos.test@STUDIO.TEST}"
external_broker_principal="${KAFKA_EXTERNAL_BROKER_PRINCIPAL:-kafka/localhost@STUDIO.TEST}"
client_principal="${KAFKA_CLIENT_PRINCIPAL:-studio-client@STUDIO.TEST}"

require_supported_realm() {
    if [[ "${realm}" != "STUDIO.TEST" ]]; then
        echo "This test image is configured for realm STUDIO.TEST, received: ${realm}" >&2
        exit 2
    fi
}

principal_exists() {
    local principal="$1"
    kadmin.local -r "${realm}" -q "get_principal ${principal}" 2>/dev/null \
        | grep -Fq "Principal: ${principal}"
}

ensure_principal() {
    local principal="$1"
    if ! principal_exists "${principal}"; then
        kadmin.local -r "${realm}" -q "add_principal -randkey ${principal}"
    fi
}

export_keytab() {
    local output="$1"
    shift
    local temporary="${output}.tmp"
    rm -f "${temporary}"
    kadmin.local -r "${realm}" -q "ktadd -norandkey -k ${temporary} $*"
    chmod 0644 "${temporary}"
    mv -f "${temporary}" "${output}"
}

initialize() {
    require_supported_realm
    mkdir -p /var/lib/krb5kdc /shared
    if [[ ! -f /var/lib/krb5kdc/principal ]]; then
        kdb5_util create -s -r "${realm}" -P "${master_password}"
    fi

    ensure_principal "${broker_principal}"
    ensure_principal "${external_broker_principal}"
    ensure_principal "${client_principal}"
    export_keytab /shared/kafka.keytab "${broker_principal}" "${external_broker_principal}"
    export_keytab /shared/studio-client.keytab "${client_principal}"

    klist -kte /shared/kafka.keytab
    klist -kte /shared/studio-client.keytab
}

run_kdc() {
    require_supported_realm
    if [[ ! -f /var/lib/krb5kdc/principal ]]; then
        echo "Kerberos database is missing; run the init service first" >&2
        exit 1
    fi
    exec krb5kdc -n -r "${realm}"
}

case "${1:-run}" in
    init)
        initialize
        ;;
    run)
        run_kdc
        ;;
    *)
        exec "$@"
        ;;
esac
