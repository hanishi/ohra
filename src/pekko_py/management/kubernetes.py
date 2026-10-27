"""Kubernetes API discovery, as pekko-discovery-kubernetes-api: list the pods carrying the
service's label through the API server, with the pod's service-account token and CA, and take each
Running pod's IP and named port."""

from __future__ import annotations

import json
import logging
import os
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import quote

from ..discovery import DiscoveryTimeoutError, Lookup, Resolved, ResolvedTarget, ServiceDiscovery
from .http import get

_log = logging.getLogger("pekko_py.management.kubernetes")

_SERVICE_ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"


@dataclass(frozen=True)
class KubernetesApiSettings:
    """Pekko's ``pekko.discovery.kubernetes-api`` settings. ``pod_namespace=None`` reads
    ``KUBERNETES_NAMESPACE``, else the mounted namespace file."""

    api_ca_path: str = f"{_SERVICE_ACCOUNT}/ca.crt"
    api_token_path: str = f"{_SERVICE_ACCOUNT}/token"
    api_service_host_env_name: str = "KUBERNETES_SERVICE_HOST"
    api_service_port_env_name: str = "KUBERNETES_SERVICE_PORT"
    pod_namespace_path: str = f"{_SERVICE_ACCOUNT}/namespace"
    pod_namespace: str | None = None
    pod_domain: str = "cluster.local"
    pod_label_selector: str = "app=%s"
    use_raw_ip: bool = False
    container_name: str | None = None


class KubernetesApiError(Exception):
    """The API server could not be reached, refused the request, or answered something other than
    200."""


def targets(
    pod_list: Mapping[str, Any],
    port_name: str | None,
    namespace: str,
    pod_domain: str,
    raw_ip: bool,
    container_name: str | None,
) -> list[ResolvedTarget]:
    """KubernetesApiServiceDiscovery.targets: Running pods not being deleted, their IP and the
    named container port."""
    out: list[ResolvedTarget] = []
    items: list[Any] = pod_list.get("items") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        pod: dict[str, Any] = cast(dict[str, Any], item)
        metadata: dict[str, Any] = pod.get("metadata") or {}
        spec: dict[str, Any] = pod.get("spec") or {}
        status: dict[str, Any] = pod.get("status") or {}
        if metadata.get("deletionTimestamp") or status.get("phase") != "Running":
            continue
        if container_name is not None:
            statuses: list[dict[str, Any]] = status.get("containerStatuses") or []
            if not any(
                s.get("name") == container_name and "waiting" not in (s.get("state") or {})
                for s in statuses
            ):
                continue
        ip: str | None = status.get("podIP")
        if not ip:
            continue
        ports: list[int | None]
        if port_name is None:
            ports = [None]
        else:
            containers: list[dict[str, Any]] = spec.get("containers") or []
            ports = []
            for container in containers:
                container_ports: list[dict[str, Any]] = container.get("ports") or []
                ports.extend(
                    int(p["containerPort"])
                    for p in container_ports
                    if p.get("name") == port_name and "containerPort" in p
                )
        host = ip if raw_ip else f"{ip.replace('.', '-')}.{namespace}.pod.{pod_domain}"
        out.extend(ResolvedTarget(host, port, ip) for port in ports)
    return out


class KubernetesApiServiceDiscovery(ServiceDiscovery):
    def __init__(self, settings: KubernetesApiSettings = KubernetesApiSettings()) -> None:
        self.settings = settings

    def _read(self, path: str, name: str) -> str | None:
        try:
            with open(path, encoding="utf-8") as f:
                return f.read().strip()
        except OSError as e:
            _log.warning("Error reading %s from %s: %s", name, path, e)
            return None

    async def lookup(self, lookup: Lookup, resolve_timeout: float) -> Resolved:
        s = self.settings
        host, port = (
            os.environ.get(s.api_service_host_env_name),
            os.environ.get(s.api_service_port_env_name),
        )
        if not host or not port:
            raise KubernetesApiError(
                "Unable to form request; check Kubernetes environment (expecting env vars "
                f"{s.api_service_host_env_name}, {s.api_service_port_env_name})"
            )
        # re-read per lookup: bound tokens rotate
        token = self._read(s.api_token_path, "api-token") or ""
        namespace = (
            s.pod_namespace
            or os.environ.get("KUBERNETES_NAMESPACE")
            or self._read(s.pod_namespace_path, "pod-namespace")
        )
        if not namespace:
            raise KubernetesApiError(
                "Unable to determine the pod namespace: set pod_namespace, KUBERNETES_NAMESPACE "
                "or mount the namespace file"
            )
        selector = s.pod_label_selector % lookup.service_name
        _log.info(
            "Querying for pods with label selector: [%s]. Namespace: [%s]. Port: [%s]",
            selector,
            namespace,
            lookup.port_name,
        )
        context = ssl.create_default_context(cafile=s.api_ca_path)
        url = f"https://{host}:{port}/api/v1/namespaces/{quote(namespace)}/pods?labelSelector={quote(selector)}"
        try:
            status, body = await get(
                url,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                timeout=resolve_timeout,
                ssl_context=context,
            )
        except TimeoutError as e:
            raise DiscoveryTimeoutError(
                f"Kubernetes API request timed out after {resolve_timeout} s"
            ) from e
        except OSError as e:
            raise KubernetesApiError(
                f"Unable to reach the Kubernetes API at {host}:{port}: {e}"
            ) from e
        if status == 403:
            _log.warning(
                "Forbidden to communicate with Kubernetes API server; check RBAC settings. "
                "Response: [%s]",
                body.decode(errors="replace"),
            )
            raise KubernetesApiError(
                "Forbidden when communicating with the Kubernetes API. Check RBAC settings."
            )
        if status != 200:
            _log.warning(
                "Non-200 when communicating with Kubernetes API server. Status code: [%s]. "
                "Response body: [%s]",
                status,
                body.decode(errors="replace"),
            )
            raise KubernetesApiError(f"Non-200 from Kubernetes API server: {status}")
        pod_list: Any = json.loads(body)
        found = targets(
            pod_list, lookup.port_name, namespace, s.pod_domain, s.use_raw_ip, s.container_name
        )
        if not found and pod_list.get("items"):
            _log.info(
                "No targets found from pod list. Is the correct port name configured? Current "
                "configuration: [%s]",
                lookup.port_name,
            )
        return Resolved(lookup.service_name, tuple(found))
