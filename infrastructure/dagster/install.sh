#!/usr/bin/env bash
# EC2 bootstrap only: invoked as root by the CloudFormation user-data script.
set -euo pipefail
: "${DAGSTER_DATA_VOLUME_ID:?}" "${DAGSTER_HOME:?}"
[[ "$DAGSTER_HOME" == /var/lib/zavant/dagster ]] || exit 2
dnf install -y python3.11 python3.11-pip

# Locate ONLY the stack's newly created data volume, never an arbitrary disk.
# Nitro exposes the EBS volume ID as its NVMe serial number (without the dash).
volume_serial="${DAGSTER_DATA_VOLUME_ID//-/}"
data_device=""
for attempt in {1..120}; do
  data_device=$(lsblk -dnpo NAME,SERIAL | awk -v serial="$volume_serial" '$2 == serial {print $1}')
  [[ -n "$data_device" ]] && break
  sleep 2
done
[[ -b "$data_device" ]] || { echo "Expected EBS volume not found" >&2; exit 1; }
filesystem=$(lsblk -dn -o FSTYPE "$data_device")
if [[ -z "$filesystem" ]]; then
  # Refuse partitions/other signatures. This path is for a NEW empty volume.
  [[ $(lsblk -nr -o NAME "$data_device" | wc -l) -eq 1 ]] || exit 1
  [[ -z $(wipefs --noheadings "$data_device") ]] || exit 1
  mkfs -t xfs "$data_device"
elif [[ "$filesystem" != xfs ]]; then
  echo "Refusing to modify a non-XFS volume" >&2
  exit 1
fi
mkdir -p /var/lib/zavant
volume_uuid=$(blkid -s UUID -o value "$data_device")
# Fail boot if the data volume is missing; never silently start with empty state.
if ! findmnt --mountpoint /var/lib/zavant >/dev/null; then
  echo "UUID=$volume_uuid /var/lib/zavant xfs defaults 0 2" >> /etc/fstab
  mount /var/lib/zavant
fi
[[ $(findmnt -n -o UUID --mountpoint /var/lib/zavant) == "$volume_uuid" ]] || exit 1

id dagster >/dev/null 2>&1 || useradd --system --create-home --home-dir /home/dagster dagster
install -d -o dagster -g dagster -m 0750 "$DAGSTER_HOME" /etc/zavant
# Preserve instance state/configuration if deliberately reusing a data volume.
if [[ ! -e "$DAGSTER_HOME/dagster.yaml" ]]; then
  install -o dagster -g dagster -m 0640 \
    /opt/zavant/app/infrastructure/dagster/dagster-service.yaml "$DAGSTER_HOME/dagster.yaml"
fi
chown -R dagster:dagster /opt/zavant/app
runuser -u dagster -- python3.11 -m venv /opt/zavant/app/.venv
runuser -u dagster -- /opt/zavant/app/.venv/bin/pip install \
  --constraint /opt/zavant/app/constraints.txt --editable '/opt/zavant/app[orchestration]'
install -m 0644 /opt/zavant/app/infrastructure/dagster/zavant-dagster*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now zavant-dagster@code zavant-dagster@webserver zavant-dagster@daemon
# systemd's simple services report started before the app is ready. Verify both
# the loaded code and UI before declaring CloudFormation bootstrap successful.
for attempt in {1..60}; do
  if /opt/zavant/app/.venv/bin/dagster api grpc-health-check -h 127.0.0.1 -p 4000 \
      && curl --fail --silent http://127.0.0.1:3000/server_info >/dev/null; then
    systemctl is-active --quiet zavant-dagster@daemon
    exit 0
  fi
  sleep 2
done
exit 1
