#!/usr/bin/env python3
"""
sync.py - Sync Proxmox nodes/VMs and k3s nodes into NetBox.

Requirements:
    uv sync

Environment variables:
    NETBOX_URL           - e.g. https://netbox.yourdomain.com
    NETBOX_TOKEN         - NetBox API token
    PROXMOX_HOST         - e.g. pve1.yourdomain.com
    PROXMOX_USER         - e.g. root@pam
    PROXMOX_PASSWORD     - Proxmox password (or use token below)
    PROXMOX_TOKEN_NAME   - API token name (alternative to password)
    PROXMOX_TOKEN_VALUE  - API token value (alternative to password)
    PROXMOX_PORT         - Proxmox port (defaults to 8006)
    KUBECONFIG           - Path to kubeconfig (defaults to ~/.kube/config)

Usage:
    # Dry run — shows what would be created/updated, no writes
    uv run python sync.py --dry-run

    # Live run
    uv run python sync.py
"""

import argparse
import contextlib
import ipaddress
import logging
import os

import pynetbox
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from proxmoxer import ProxmoxAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Tracks dry-run actions for the summary at the end
_dry_run_log: list[dict] = []


def _dry(action: str, resource_type: str, name: str, detail: str = "") -> None:
    """Record and print a dry-run action."""
    entry = {"action": action, "type": resource_type, "name": name, "detail": detail}
    _dry_run_log.append(entry)
    detail_str = f" ({detail})" if detail else ""
    log.info(f"[DRY-RUN] {action}: {resource_type} '{name}'{detail_str}")


def print_dry_run_summary() -> None:
    if not _dry_run_log:
        print("\n✓ Dry run complete — nothing new to sync.")
        return

    from collections import Counter

    counts = Counter(e["action"] for e in _dry_run_log)
    print("\n" + "=" * 60)
    print("DRY-RUN SUMMARY — no changes were made")
    print("=" * 60)
    for action, count in counts.items():
        print(f"  {action}: {count}")
    print()

    for action in ("CREATE", "UPDATE", "REASSIGN", "SET_PRIMARY"):
        items = [e for e in _dry_run_log if e["action"] == action]
        if not items:
            continue
        print(f"{action}:")
        for e in items:
            detail_str = f" — {e['detail']}" if e["detail"] else ""
            print(f"  [{e['type']}] {e['name']}{detail_str}")
        print()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize_cidr(address: str) -> str:
    """Normalize an IP address to CIDR notation with the correct prefix length."""
    if "/" in address:
        return address

    with contextlib.suppress(ValueError):
        ip = ipaddress.ip_address(address)
        # Tailscale CGNAT range — always /32
        if ip in ipaddress.ip_network("100.64.0.0/10"):
            return f"{address}/32"
    return f"{address}/24"


def is_lan_ip(address: str) -> bool:
    """Return True if address is a private LAN IP (not Tailscale, not pod network)."""
    with contextlib.suppress(ValueError):
        ip = ipaddress.ip_address(address.split("/")[0])
        tailscale = ipaddress.ip_network("100.64.0.0/10")
        pod_network = ipaddress.ip_network("10.0.0.0/8")
        link_local = ipaddress.ip_network("172.16.0.0/12")
        if ip in tailscale or ip in pod_network or ip in link_local:
            return False
        return ip.is_private
    return False


def should_skip_interface(name: str) -> bool:
    exact = {"lo", "flannel.1", "cni0", "kube-ipvs0", "dummy0", "docker0", "hassio"}
    prefixes = ("br-", "veth")
    return name in exact or name.startswith(prefixes)


# ---------------------------------------------------------------------------
# NetBox helpers
# ---------------------------------------------------------------------------


def get_nb() -> pynetbox.api:
    url = os.environ["NETBOX_URL"]
    token = os.environ["NETBOX_TOKEN"]
    nb = pynetbox.api(url, token=token)
    nb.http_session.verify = True  # set False if using self-signed cert
    return nb


def upsert_device(
    nb,
    name: str,
    role_slug: str,
    platform_slug: str,
    site_slug: str,
    dry_run: bool = False,
) -> object:
    """Create or retrieve a NetBox device by name."""
    if existing := nb.dcim.devices.get(name=name):
        log.info(f"EXISTS device: {name}")
        return existing

    if dry_run:
        _dry(
            "CREATE",
            "Device",
            name,
            f"role={role_slug}, platform={platform_slug}, site={site_slug}",
        )
        return _MockDevice(name)

    # Resolve / create foreign keys
    role = nb.dcim.device_roles.get(slug=role_slug) or nb.dcim.device_roles.create(
        name=role_slug.replace("-", " ").title(), slug=role_slug, color="0000ff"
    )

    device_type = nb.dcim.device_types.get(slug="generic-server")
    if not device_type:
        manufacturer = nb.dcim.manufacturers.get(
            slug="generic"
        ) or nb.dcim.manufacturers.create(name="Generic", slug="generic")
        device_type = nb.dcim.device_types.create(
            manufacturer=manufacturer.id, model="Generic Server", slug="generic-server"
        )

    site = nb.dcim.sites.get(slug=site_slug) or nb.dcim.sites.create(
        name=site_slug.replace("-", " ").title(), slug=site_slug
    )

    platform = nb.dcim.platforms.get(slug=platform_slug) or nb.dcim.platforms.create(
        name=platform_slug.replace("-", " ").title(), slug=platform_slug
    )

    device = nb.dcim.devices.create(
        name=name,
        device_type=device_type.id,
        role=role.id,
        platform=platform.id,
        site=site.id,
        status="active",
    )
    log.info(f"CREATED device: {name}")
    return device


def upsert_ip(
    nb, address: str, device, interface_name: str = "eth0", dry_run: bool = False
) -> object | None:
    """
    Assign an IP to a device interface in NetBox, creating if needed.
    Returns the IP address object (or None) so callers can set it as primary.
    """
    if not address:
        return None

    cidr = normalize_cidr(address)

    if isinstance(device, _MockDevice):
        _dry(
            "CREATE", "IPAddress", cidr, f"device={device.name}, iface={interface_name}"
        )
        return None

    iface = nb.dcim.interfaces.get(device_id=device.id, name=interface_name)
    iface_is_new = iface is None
    existing_ip = nb.ipam.ip_addresses.get(address=cidr)

    if dry_run:
        if iface_is_new:
            _dry("CREATE", "Interface", f"{device.name}/{interface_name}", "")
        if existing_ip:
            if existing_ip.assigned_object_id != (iface.id if iface else None):
                _dry(
                    "REASSIGN", "IPAddress", cidr, f"-> {device.name}/{interface_name}"
                )
            else:
                log.info(f"EXISTS ip: {cidr} on {device.name}/{interface_name}")
        else:
            _dry(
                "CREATE",
                "IPAddress",
                cidr,
                f"device={device.name}, iface={interface_name}",
            )
        # Return cidr string so callers can still pass it to set_primary_ip in dry-run
        return cidr

    if iface_is_new:
        iface = nb.dcim.interfaces.create(
            device=device.id, name=interface_name, type="1000base-t"
        )

    if existing_ip:
        if existing_ip.assigned_object_id != iface.id:
            existing_ip.assigned_object_type = "dcim.interface"
            existing_ip.assigned_object_id = iface.id
            existing_ip.save()
            log.info(f"REASSIGNED ip: {cidr} -> {device.name}/{interface_name}")
        else:
            log.info(f"EXISTS ip: {cidr} on {device.name}/{interface_name}")
        return existing_ip

    ip = nb.ipam.ip_addresses.create(
        address=cidr,
        status="active",
        assigned_object_type="dcim.interface",
        assigned_object_id=iface.id,
    )
    log.info(f"CREATED ip: {cidr} on {device.name}/{interface_name}")
    return ip


def set_primary_ip(nb, device, ip_obj_or_cidr, dry_run: bool = False) -> None:
    """Set the primary IPv4 on a device if not already set.

    ip_obj_or_cidr: a live pynetbox IP object (live run) or a CIDR string (dry-run fallback).
    """
    if isinstance(device, _MockDevice) or ip_obj_or_cidr is None:
        return

    # Re-fetch device to get current primary_ip state
    current = nb.dcim.devices.get(device.id)
    if current.primary_ip4:
        log.info(f"EXISTS primary_ip: {device.name} already has {current.primary_ip4}")
        return

    display = (
        ip_obj_or_cidr if isinstance(ip_obj_or_cidr, str) else ip_obj_or_cidr.address
    )

    if dry_run:
        _dry("SET_PRIMARY", "Device", device.name, f"primary_ip4={display}")
        return

    current.primary_ip4 = ip_obj_or_cidr.id
    current.save()
    log.info(f"SET primary_ip: {device.name} -> {display}")


# ---------------------------------------------------------------------------
# Mock device for dry-run pass-through
# ---------------------------------------------------------------------------


class _MockDevice:
    """Placeholder returned during dry-run when a device doesn't exist yet."""

    def __init__(self, name: str):
        self.name = name
        self.id = None


# ---------------------------------------------------------------------------
# Proxmox sync
# ---------------------------------------------------------------------------


def get_proxmox() -> ProxmoxAPI:
    host = os.environ["PROXMOX_HOST"]
    user = os.environ["PROXMOX_USER"]
    token_name = os.environ.get("PROXMOX_TOKEN_NAME")
    token_value = os.environ.get("PROXMOX_TOKEN_VALUE")
    password = os.environ.get("PROXMOX_PASSWORD")
    port = int(os.environ.get("PROXMOX_PORT", 8006))
    timeout = int(os.environ.get("PROXMOX_TIMEOUT", 10))

    if token_name and token_value:
        return ProxmoxAPI(
            host,
            user=user,
            token_name=token_name,
            token_value=token_value,
            verify_ssl=False,
            port=port,
            timeout=timeout,
        )
    return ProxmoxAPI(
        host, user=user, password=password, verify_ssl=False, port=port, timeout=timeout
    )


def sync_proxmox(
    nb, proxmox: ProxmoxAPI, site_slug: str = "homelab", dry_run: bool = False
) -> None:
    log.info("--- Syncing Proxmox ---")

    for node in proxmox.nodes.get():
        node_name = node["node"]
        log.info(f"Processing Proxmox node: {node_name}")

        node_site_slug = "home" if node_name.startswith("pve") else site_slug

        device = upsert_device(
            nb,
            name=node_name,
            role_slug="proxmox-node",
            platform_slug="proxmox",
            site_slug=node_site_slug,
            dry_run=dry_run,
        )

        try:
            network = proxmox.nodes(node_name).network.get()
            for iface in network:
                if iface.get("type") == "bridge" and iface.get("address"):
                    ip_obj = upsert_ip(
                        nb,
                        iface["address"],
                        device,
                        interface_name=iface.get("iface", "vmbr0"),
                        dry_run=dry_run,
                    )
                    set_primary_ip(nb, device, ip_obj, dry_run=dry_run)
                    break
        except Exception as e:
            log.warning(f"Could not fetch network for {node_name}: {e}")

        # VMs
        for vm in proxmox.nodes(node_name).qemu.get():
            if vm.get("template") == 1:
                log.info(f"  Skipping template: {vm.get('name') or vm['vmid']}")
                continue

            vm_name = vm.get("name") or f"vm-{vm['vmid']}"

            if vm_name.startswith("k3s-"):
                log.info(f"  Skipping {vm_name} — will be synced by k3s pass")
                continue

            log.info(f"  Processing VM: {vm_name}")
            vm_device = upsert_device(
                nb,
                name=vm_name,
                role_slug="virtual-machine",
                platform_slug="linux",
                site_slug=site_slug,
                dry_run=dry_run,
            )

            primary_candidate = None
            try:
                agent_info = (
                    proxmox.nodes(node_name)
                    .qemu(vm["vmid"])
                    .agent("network-get-interfaces")
                    .get()
                )
                for iface in agent_info.get("result", []):
                    if should_skip_interface(iface.get("name", "")):
                        continue
                    for ip_info in iface.get("ip-addresses", []):
                        if ip_info.get("ip-address-type") == "ipv4":
                            ip_obj = upsert_ip(
                                nb,
                                ip_info["ip-address"],
                                vm_device,
                                interface_name=iface["name"],
                                dry_run=dry_run,
                            )
                            # First LAN IP wins as primary candidate
                            if primary_candidate is None and is_lan_ip(
                                ip_info["ip-address"]
                            ):
                                primary_candidate = ip_obj
                            break
            except Exception:
                log.debug(f"  Guest agent not available for {vm_name}, skipping IP")

            set_primary_ip(nb, vm_device, primary_candidate, dry_run=dry_run)

        # LXCs
        for ct in proxmox.nodes(node_name).lxc.get():
            ct_name = ct.get("name") or f"ct-{ct['vmid']}"
            log.info(f"  Processing LXC: {ct_name}")
            ct_device = upsert_device(
                nb,
                name=ct_name,
                role_slug="container",
                platform_slug="linux",
                site_slug=site_slug,
                dry_run=dry_run,
            )

            primary_candidate = None
            try:
                ifaces = proxmox.nodes(node_name).lxc(ct["vmid"]).interfaces.get()
                for iface in ifaces:
                    if should_skip_interface(iface.get("name", "")):
                        continue
                    if iface.get("inet"):
                        ip_obj = upsert_ip(
                            nb,
                            iface["inet"],
                            ct_device,
                            interface_name=iface["name"],
                            dry_run=dry_run,
                        )
                        if primary_candidate is None and is_lan_ip(iface["inet"]):
                            primary_candidate = ip_obj
            except Exception:
                log.debug(f"  Could not get interfaces for LXC {ct_name}")

            set_primary_ip(nb, ct_device, primary_candidate, dry_run=dry_run)


# ---------------------------------------------------------------------------
# k3s sync
# ---------------------------------------------------------------------------


def sync_k3s(nb, site_slug: str = "homelab", dry_run: bool = False) -> None:
    log.info("--- Syncing k3s ---")
    kubeconfig = os.environ.get("KUBECONFIG", os.path.expanduser("~/.kube/config"))
    k8s_config.load_kube_config(config_file=kubeconfig)
    v1 = k8s_client.CoreV1Api()

    for node in v1.list_node().items:
        node_name = node.metadata.name
        labels = node.metadata.labels or {}

        role_slug = (
            "k3s-control-plane"
            if "node-role.kubernetes.io/master" in labels
            or "node-role.kubernetes.io/control-plane" in labels
            else "k3s-worker"
        )
        log.info(f"Processing k3s node: {node_name} ({role_slug})")

        device = upsert_device(
            nb,
            name=node_name,
            role_slug=role_slug,
            platform_slug="linux",
            site_slug=site_slug,
            dry_run=dry_run,
        )

        for addr in node.status.addresses:
            if addr.type == "InternalIP":
                ip_obj = upsert_ip(
                    nb, addr.address, device, interface_name="eth0", dry_run=dry_run
                )
                set_primary_ip(nb, device, ip_obj, dry_run=dry_run)
                break


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Proxmox and k3s inventory into NetBox."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be created or updated without writing anything to NetBox.",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info("*** DRY-RUN MODE — no changes will be written to NetBox ***")

    nb = get_nb()
    proxmox = get_proxmox()

    sync_proxmox(nb, proxmox, site_slug="homelab", dry_run=args.dry_run)
    sync_k3s(nb, site_slug="homelab", dry_run=args.dry_run)

    if args.dry_run:
        print_dry_run_summary()
    else:
        log.info("Sync complete.")


if __name__ == "__main__":
    main()
