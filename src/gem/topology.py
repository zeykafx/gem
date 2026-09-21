from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml


def parse_walltime(value) -> timedelta:
    return pd.Timedelta(value).to_pytimedelta()


@dataclass
class Server:
    cluster: str
    nodes: int = 1
    # Allow for a specific node (optional), e.g. "chirop-5.lille.grid5000.fr"
    node: Optional[str] = None


@dataclass
class Site:
    name: str
    cluster: str
    num_machines: int = 1

@dataclass
class AddRole:
    name: str
    sites: list[Site]


@dataclass
class Topology:
    name: str
    wall_time: timedelta
    netns_per_client: int
    frrouting_version: str
    router_template: str
    relay_nodes: bool
    server: Server
    sites: list[Site]
    links: list[tuple[str, str]]
    additional_roles: list[AddRole]

    @staticmethod
    def client_role(site_name: str) -> str:
        return f"client_{site_name}"

    @staticmethod
    def relay_role(site_name: str) -> str:
        return f"relay_{site_name}"

    # all clusters with clients, each with number of clients
    @property
    def client_clusters(self) -> list[dict]:
        return [
            {"cluster": s.cluster, "num_clients": s.num_machines} for s in self.sites
        ]

    @property
    def topology_links(self) -> list[tuple[str, str]]:
        return self.links


def load_topology(path: str | Path) -> Topology:
    path = Path(path)
    raw = yaml.safe_load(path.read_text()) or {}

    # allow both a file wrapped in `topology:` and a bare one
    topo_raw = raw.get("topology", raw)

    name = topo_raw.get("name")
    if not name:
        raise ValueError(f"{path}: missing 'topology.name'")

    # if walltime is missing, set it to 1hr
    walltime = parse_walltime(topo_raw.get("wall_time", "1hr"))

    use_relays: bool = topo_raw.get("relay_nodes", False)
    netns_per_client: int = topo_raw.get("netns_per_client", 5)
    router_template: str = topo_raw.get(
        "router_template", "base_router_config_ospf.frr"
    )
    frr_ver = topo_raw.get("frrouting_version", "frr-10.4")

    server_raw = topo_raw.get("server") or {}
    server = Server(
        cluster=server_raw["cluster"],
        nodes=int(
            server_raw.get("nodes", 1)
        ),  # default to 1 server nodes, doesn't really work with more than 1
        node=server_raw.get("node"),
    )

    sites = [
        Site(
            name=site["name"],
            cluster=site["cluster"],
            num_machines=int(site.get("number", 1)),  # default to 1 client
        )
        for site in (topo_raw.get("sites") or [])
    ]

    links = [tuple(l) for l in (topo_raw.get("links") or [])]

    additional_roles_raw = topo_raw.get("additional_roles") or []

    additional_roles = []
    for add_role in additional_roles_raw:
        add_role_sites = [
            Site(
                name=site["name"],
                cluster=site["cluster"],
                num_machines=int(site.get("number", 1))
            )
            for site in (add_role["sites"] or[])
        ]
        
        additional_roles.append(AddRole(
            name=add_role["role"],
            sites=add_role_sites,
        ))

    topo = Topology(
        name=name,
        wall_time=walltime,
        server=server,
        sites=sites,
        links=links,
        relay_nodes=use_relays,
        netns_per_client=netns_per_client,
        frrouting_version=frr_ver,
        router_template=router_template,
        additional_roles=additional_roles,
    )
    return topo
