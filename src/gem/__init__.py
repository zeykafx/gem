from collections import defaultdict
from datetime import datetime, timedelta
from itertools import islice
import logging
from typing import Sequence
import os
from pathlib import Path
import subprocess
import concurrent.futures
from grid5000 import Grid5000
import enoslib as en
from ipaddress import ip_address, ip_network
from pathlib import Path
from jinja2 import Template
from ipaddress import ip_address, ip_network
from .topology import Topology, load_topology


class G5KExpe:
    def __init__(
        self,
        topology_conf: str | Path,
        g5k_conf_file_loc: str = ".python-grid5000.yaml",
        job_type: str = "deploy",
        os_env_name: str = "debian12-nfs",
        ansible_forks: int = 25,
    ):
        """
        Creates an instance of G5KExpe
        - `topology_conf`: path of the topology YAML file
        - `g5k_conf_file_loc`: path of the python-grid5000 file containing your G5K identifiers
        - `job_type`: defines the type of your job, likely should be "deploy"
        - `os_env_name`: name of the environment, see list here: https://www.grid5000.fr/w/Getting_Started#:~:text=On%20Grid%275000%20reference%20environments%5Bedit%5D
        - `ansible_forks`: configure Ansible's "forks" parameter. Ansible's default value is 5 (i.e. it executes a task on the first 5 hosts, waits for completion, then executes on the next five hosts, ...). Here the default value is 25.
           Warning: don't set this value too high, as it can use a lot more resources
        """
        conf_file = os.path.join(os.environ.get("HOME"), g5k_conf_file_loc)  # type: ignore
        gk = Grid5000.from_yaml(conf_file)

        self.topology: Topology = load_topology(path=topology_conf)

        # performance tuning: see https://discovery.gitlabpages.inria.fr/enoslib/tutorials/performance_tuning.html#performance-tuning ---------
        en.set_config(ansible_forks=ansible_forks)
        # Enable Ansible pipelining
        os.environ["ANSIBLE_PIPELINING"] = "True"
        os.environ["ANSIBLE_HOST_KEY_CHECKING"] = "False"

        # ---------------

        self.job_type = job_type
        self.env_name = os_env_name

        # map each cluster to its site
        self.cluster_to_site = {}
        for site in gk.sites.list():
            for cluster in site.clusters.list():
                self.cluster_to_site[cluster.uid] = site.uid

        self.roles: en.Roles
        self.networks: en.Networks

        self.prod_interfaces_per_node = {}
        self.subnet_cluster_mapping = {}

        self.node_ips = {}
        self.all_ns_ips = []

        self.gateway_ip_per_cluster = {}

        self.current_provider: en.G5k | None = None
        self.router_tunnels = defaultdict(list)

    def usage_policy_check(self):
        datetime_now = datetime.now()
        job_end_dt = datetime_now + self.topology.wall_time
        if datetime_now.hour <= 17 and job_end_dt.hour >= 19 and datetime_now.weekday() < 5:
            raise RuntimeError(
                "This job reservation will violate the usage policy and will cross the day night boundary"
            )

    def setup_enoslib_conf(self) -> en.G5k:
        """
        Sets up the enoslib reservation for the test as defined in the topology file
        Returns the G5k `provider` object containing the machines to reserve,...
        """
        # Display some general information about the library
        en.check()
        # Enable rich logging
        _ = en.init_logging()

        logging.getLogger("paramiko").setLevel(logging.WARNING)

        server_cluster = self.cluster_to_site[self.topology.server.cluster]

        conf = (
            en.G5kConf.from_settings(
                job_name=self.topology.name,
                walltime=str(self.topology.wall_time),
                env_name=self.env_name,
                job_type=[self.job_type],
            )
            # server router
            .add_machine(
                roles=["router", "router_server"],
                cluster=self.topology.server.cluster,
                nodes=1,
            ).add_network(
                id="subnet_server",
                type="slash_22",
                roles=["subnet", "subnet_server"],
                site=server_cluster,
            )
        )

        if self.topology.server.node is not None:
            conf = conf.add_machine(
                roles=["server"],
                servers=[self.topology.server.node],
                nodes=self.topology.server.nodes,
            )
        else:
            conf = conf.add_machine(
                roles=["server"],
                cluster=self.topology.server.cluster,
                nodes=self.topology.server.nodes,
            )

        # we need to add one client router + clients + relay + subnet for each client cluster

        for i, client_cluster in enumerate(self.topology.client_clusters):
            # print(client_cluster)

            conf = (
                conf
                # add only one client router
                .add_machine(
                    roles=["router", "router_client", f"router_client_{i}"],
                    cluster=client_cluster["cluster"],
                    nodes=1,
                )
                # add all of the client machines
                .add_machine(
                    roles=["client", f"client_{i}"],
                    cluster=client_cluster["cluster"],
                    nodes=client_cluster["num_clients"],
                ).add_network(
                    id=f"subnet_client_{i}",
                    type="slash_22",
                    roles=["subnet", "subnet_client", f"subnet_client_{i}"],
                    site=self.cluster_to_site[client_cluster["cluster"]],
                )
            )
            if self.topology.relay_nodes is True:
                # if relays are used, then add one relay machine per client cluster
                conf = conf.add_machine(
                    roles=["relay", f"relay_{i}"],
                    cluster=client_cluster["cluster"],
                    nodes=1,
                )

        # This will validate the configuration, but not reserve resources yet
        provider = en.G5k(conf)
        self.current_provider = provider
        return provider

    def reserve_res(self, provider: en.G5k):
        """
        Reserves the resources as defined in `provider`
        Returns the roles obtained (or not) following the reservation
        """
        print("Reserving resources now, might take a while...")

        # Get actual resources
        self.roles, self.networks = provider.init()

        print("Obtained resources:")
        print(f"Roles: {self.roles}")
        print(f"Networks: {self.networks}")

        # Fill in network information from nodes
        self.roles = en.sync_info(self.roles, self.networks)

        with en.actions(roles=self.roles) as a:
            a.apt(task_name="Install traceroute", name="traceroute", state="present")
            a.apt(task_name="Install btop", name="btop", state="present")
            a.apt(task_name="Install htop", name="htop", state="present")
            a.apt(task_name="Install tcpdump", name="tcpdump", state="present")
            a.apt(
                task_name="Install python",
                name=["python3-pip", "python-is-python3"],
                state="present",
            )

        with en.actions(roles=self.roles["router"], gather_facts=True) as a:
            a.file(
                task_name="Ensure apt keyring directory exists",
                path="/usr/share/keyrings",
                state="directory",
                mode="0755",
            )
            a.get_url(
                task_name="Download FRR GPG key",
                url="https://deb.frrouting.org/frr/keys.gpg",
                dest="/usr/share/keyrings/frrouting.gpg",
                mode="0644",
            )
            a.apt_repository(
                task_name="Add FRR apt repository",
                repo="deb [signed-by=/usr/share/keyrings/frrouting.gpg] https://deb.frrouting.org/frr {{ ansible_distribution_release }} "
                + self.topology.frrouting_version,
                filename="frr",
                state="present",
            )
            a.apt(
                task_name="Install FRR packages",
                name=["frr", "frr-pythontools"],
                state="present",
                update_cache=True,
            )
            results = a.results
            print(f"Results : {results}")

    def sync_info(self):
        self.roles = en.sync_info(self.roles, self.networks)

    def setup_interfaces(self):

        self.prod_interfaces_per_node = {}

        # find the physical interface connected to the production network

        for host in (
            self.roles["client"]
            + self.roles["server"]
            + self.roles["router"]
            + self.roles["relay"]
        ):
            node_name = host.address

            prod_interfaces = host.filter_interfaces(networks=self.networks["prod"])
            if prod_interfaces:
                prod_interface_name = prod_interfaces[0]
                print(f"Prod interface for {host.alias}: {prod_interface_name}")
                self.prod_interfaces_per_node[host.alias] = prod_interface_name

            else:
                print(
                    f"Couldn't find prod iface for {host.alias}, checking each interface directly"
                )
                prod_network = ip_network("172.0.0.0/8")
                for interface in host.net_devices:
                    for address in interface.addresses:
                        if address.ip in prod_network:
                            self.prod_interfaces_per_node[host.alias] = interface.name

        print(f"Production interfaces per node: {self.prod_interfaces_per_node}")

        self.subnet_cluster_mapping = {}
        for i, client_cluster in enumerate(self.topology.client_clusters):
            site = client_cluster["cluster"]
            subnet = self.networks[f"subnet_client_{i}"][0].network
            self.subnet_cluster_mapping[site] = str(subnet.network_address)

        print(f"Mapping of subnet to clusters: {self.subnet_cluster_mapping}")

    def assign_n_ips_to_hosts(self, role: str, N: int, ips):

        for host in self.roles[role]:

            host_prod_iface = self.prod_interfaces_per_node[host.alias]
            host.extra.update(ips=[str(ip) for ip in islice(ips, N)])

            for ip in host.extra.get("ips"):

                print(f"Adding ip {ip} to host: {host.alias}")

                if self.node_ips.get(role) is None:
                    self.node_ips[role] = []
                self.node_ips[role].append(ip)

                if "router" not in role:
                    cmd = f"(ip a | grep {ip}) || ip addr add {ip}/32 dev {host_prod_iface}"
                    en.run_command(cmd, task_name="cmd", roles=host, gather_facts=False)

            if "router" in role:

                # get each node's IP address on the production network
                ip_address_list = host.filter_addresses(networks=self.networks["prod"])
                if len(ip_address_list) > 0:
                    ip_address_obj = ip_address_list[0]
                else:
                    # if we cant obtain info from the host's production network (it's buggy in LLN), then fetch the ip directly
                    prod_network = ip_network("172.0.0.0/8")
                    for interface in host.net_devices:
                        for address in interface.addresses:
                            if address.ip in prod_network:
                                ip_address_obj = address

                # from enoslib tutorial:
                # This may seem weird: ip_address_obj.ip is a `netaddr.IPv4Interface`
                # which itself has an `ip` attribute.
                node_ip = ip_address_obj.ip.ip
                if self.node_ips.get(role) is None:
                    self.node_ips[role] = []
                self.node_ips[role].append(node_ip.exploded)
                host.extra.update(ips=self.node_ips[role])

    def assign_ns_ips_to_clients(self, role: str, ips):
        # assign ip addresses of the client nodes

        for host in self.roles[role]:
            ns_ips = [str(ip) for ip in islice(ips, self.topology.netns_per_client)]
            host.extra.update(ips=ns_ips)
            host.extra.update(
                ns_configs=[{"id": j, "ip": ns_ips[j]} for j in range(len(ns_ips))]
            )

            if self.node_ips.get(role) is None:
                self.node_ips[role] = []
            self.node_ips[role].extend(ns_ips)
            self.all_ns_ips.extend(ns_ips)

            print(
                f"Allocated {len(ns_ips)} namespace IP addresses for {host.alias}: {ns_ips}"
            )

    def assign_node_ips(self):

        self.node_ips = {}
        # all namespace IPs across all client nodes (flat list for passing to NPF)
        self.all_ns_ips = []

        self.server_ips = self.networks["subnet_server"][0].free_ips

        self.assign_n_ips_to_hosts("router_server", 1, self.server_ips)
        self.assign_n_ips_to_hosts("server", 1, self.server_ips)

        # assign ips to all clients, relays, and routers in each cluster
        for i, client_cluster in enumerate(self.topology.client_clusters):
            client_ips = self.networks[f"subnet_client_{i}"][0].free_ips
            self.assign_n_ips_to_hosts(f"router_client_{i}", 1, client_ips)
            self.assign_ns_ips_to_clients(f"client_{i}", client_ips)
            # one IP for the relay in this cluster
            self.assign_n_ips_to_hosts(f"relay_{i}", 1, client_ips)

        print(f"Node IPs: {self.node_ips}")
        print(
            f"number of total ip addresses (i.e., indiviual client): {len(self.all_ns_ips)}"
        )
        print(f"All Network namespaces IPs: {self.all_ns_ips}")

    def netns_setup_macvlan(self):
        # creating the namespaces:
        # we need the gateway IP per cluster for the default routes of the namespaces
        # the router's subnet IP (10.xzy) is in the same /22 subnet as the namespace IPS
        # so we use that as the gateway (the global/prod IP is on a different subnet).
        self.gateway_ip_per_cluster = {}
        for i in range(len(self.topology.client_clusters)):
            router_role = f"router_client_{i}"
            local_subnet = ip_network("10.0.0.0/8")
            host_ips = [
                str(ip)
                for ip in self.node_ips[router_role]
                if ip_address(ip) in local_subnet
            ]
            if not host_ips:
                raise RuntimeError(f"No subnet IP for {router_role}")
            self.gateway_ip_per_cluster[i] = host_ips[0]

        for i, client_cluster in enumerate(self.topology.client_clusters):
            role = f"client_{i}"
            gateway_ip = self.gateway_ip_per_cluster[i]
            print(f"gateway_ip={gateway_ip} for client {role}")

            for host in self.roles[role]:
                prod_iface = self.prod_interfaces_per_node[host.alias]
                # store prod_iface and gateway in extra so we can use them in the jinja template of en.play_on (see enoslib docs on ansible)
                host.extra.update(prod_iface=prod_iface, ns_gateway=gateway_ip)

            with en.play_on(
                roles=self.roles, pattern_hosts=role, gather_facts=False
            ) as p:
                # NOTE: the commands below will run as root iif the ssh keys setup in g5k are present on the current machine
                p.shell(
                    """
                    NS_NAME="client-{{ item.id }}"
                    MACVLAN_HOST="mv-c{{ item.id }}"
                    MACVLAN_NS="eth0"
                    IP_ADDR="{{ item.ip }}"
                    PROD_IFACE="{{ prod_iface }}"
                    GATEWAY="{{ ns_gateway }}"

                    ip netns add "$NS_NAME"

                    # create a MACVLAN interface on the prod interface
                    ip link add "$MACVLAN_HOST" link "$PROD_IFACE" type macvlan mode bridge
                    ip link set "$MACVLAN_HOST" netns "$NS_NAME"

                    # configure the netns interface
                    ip netns exec "$NS_NAME" ip link set "$MACVLAN_HOST" name "$MACVLAN_NS"
                    ip netns exec "$NS_NAME" ip addr add "$IP_ADDR"/22 dev "$MACVLAN_NS"
                    ip netns exec "$NS_NAME" ip link set "$MACVLAN_NS" up
                    ip netns exec "$NS_NAME" ip link set lo up
                    ip netns exec "$NS_NAME" ip link set "$MACVLAN_NS" multicast on
                    ip netns exec "$NS_NAME" ip route add default via "$GATEWAY" dev "$MACVLAN_NS"

                    sysctl -w net.core.rmem_max=26214400
                    sysctl -w net.core.rmem_default=26214400
                    """,
                    loop="{{ ns_configs }}",
                    task_name="create_macvlan_namespaces",
                )

            print(
                f"Created {len(self.roles[role]) * self.topology.netns_per_client} namespaces for {role} (with gateway {gateway_ip})"
            )

    def get_global_ip_for_role(self, host: en.Host, role: str) -> str:
        local_subnet = ip_network(
            "10.0.0.0/8"
        )  # the subnets we get are in 10..../8, can't be more specific than that sadly
        # since we fetched the global address for the routers and assigned them a local address
        # and we want the global address, we filter out the local address
        host_ips = [
            str(ip)
            for ip in host.extra.get("ips", [])
            if ip_address(ip) not in local_subnet
        ]
        if host_ips:
            return host_ips[0]
        raise RuntimeError(f"Could get prod ip for '{host.address}'")

    def setup_gre_tunnels(self):
        # per router tunnel info: router_tunnels[role] -> list of {iface, ip, network, tunnel_subnet}
        # used for the FRRouting configuration
        self.router_tunnels = defaultdict(list)

        # per router GRE interface counter (each router gets gre1, gre2, ... for each link it participates in)
        gre_counter = defaultdict(int)

        # per router GRE shell commands
        # NOTE: we execute everthing at the same time otherwise there can be issues with reachability
        router_gre_cmds = defaultdict(list)

        tunnel_base = int(ip_address("192.168.0.0"))

        for link_idx, (role_a, role_b) in enumerate(self.topology.links):

            host_a = self.roles[role_a][0]
            host_b = self.roles[role_b][0]

            prod_ip_a = self.get_global_ip_for_role(host_a, role_a)
            prod_ip_b = self.get_global_ip_for_role(host_b, role_b)

            # /30 tunnel subnet for this link
            tunnel_subnet = ip_network((tunnel_base + link_idx * 4, 30))
            tunnel_ip_a = str(tunnel_subnet.network_address + 1)
            tunnel_ip_b = str(tunnel_subnet.network_address + 2)

            # GRE interface names
            gre_counter[role_a] += 1
            gre_counter[role_b] += 1
            gre_iface_a = f"gre{gre_counter[role_a]}"
            gre_iface_b = f"gre{gre_counter[role_b]}"

            # tunnel metadata used in FRR config
            self.router_tunnels[role_a].append(
                {
                    "iface": gre_iface_a,
                    "ip": tunnel_ip_a,
                    "network": str(tunnel_subnet.network_address),
                    "tunnel_subnet": tunnel_subnet,
                }
            )
            self.router_tunnels[role_b].append(
                {
                    "iface": gre_iface_b,
                    "ip": tunnel_ip_b,
                    "network": str(tunnel_subnet.network_address),
                    "tunnel_subnet": tunnel_subnet,
                }
            )

            # GRE commands for side A
            router_gre_cmds[role_a].extend(
                [
                    f"sudo ip link del {gre_iface_a} 2>/dev/null || true",
                    f"sudo ip tunnel add {gre_iface_a} mode gre local {prod_ip_a} remote {prod_ip_b} ttl 255",
                    f"sudo ip addr add {tunnel_ip_a}/30 dev {gre_iface_a}",
                    f"sudo ip link set {gre_iface_a} up",
                    f"sudo ip link set {gre_iface_a} multicast on",
                    f"sudo sysctl -w net.ipv4.conf.{gre_iface_a}.rp_filter=0",
                    "sudo sysctl -w net.ipv4.conf.all.rp_filter=0",
                ]
            )

            # GRE commands for side B
            router_gre_cmds[role_b].extend(
                [
                    f"sudo ip link del {gre_iface_b} 2>/dev/null || true",
                    f"sudo ip tunnel add {gre_iface_b} mode gre local {prod_ip_b} remote {prod_ip_a} ttl 255",
                    f"sudo ip addr add {tunnel_ip_b}/30 dev {gre_iface_b}",
                    f"sudo ip link set {gre_iface_b} up",
                    f"sudo ip link set {gre_iface_b} multicast on",
                    f"sudo sysctl -w net.ipv4.conf.{gre_iface_b}.rp_filter=0",
                    "sudo sysctl -w net.ipv4.conf.all.rp_filter=0",
                ]
            )

            print(
                f"Link {link_idx}: {gre_iface_a}({role_a}, {tunnel_ip_a}) <-> {gre_iface_b}({role_b}, {tunnel_ip_b})"
            )

        for role, cmds in router_gre_cmds.items():
            host = self.roles[role][0]
            en.run_command(
                "; ".join(cmds),
                task_name=f"setup_gre_{role}",
                roles=host,
                gather_facts=False,
            )
            print(
                f"Created {len(self.router_tunnels[role])} GRE tunnels on {role} ({host.address})"
            )

        print(f"Router tunnels: {dict(self.router_tunnels)}")

    def parse_subnet(self, subnet_obj):
        for attr in ("network", "cidr", None):
            # try to get subnet_obj.attr (use the string repr of subnet_obj and parse it with ip_network if it's a string)
            val = (
                str(getattr(subnet_obj, attr, subnet_obj)) if attr else str(subnet_obj)
            )
            if not val:
                continue
            try:
                return ip_network(val if "/" in val else f"{val}/22", strict=False)
            except ValueError:
                print(f"Failed to parse {val} ip network")
                continue
        raise RuntimeError(f"Can't parse subnet: {subnet_obj!r}")

    def get_router_subnet_or_global_ip(
        self, host, role: str, get_subnet_ip: bool
    ) -> str:
        local_subnet = ip_network(
            "10.0.0.0/8"
        )  # the subnets /22 we get from g5k are all in the 10..../8 subnet, can't be more specific than that sadly
        # since we fetched the global address for the routers and assigned them a local address

        host_ips = []
        for extra_ip in host.extra.get("ips", []):
            extra_ip_addr = ip_address(extra_ip)
            # if we want to fetch the local address, then only add the address if it is in the local subnet
            if extra_ip_addr in local_subnet and get_subnet_ip:
                host_ips.append(str(extra_ip))

            # if we want to fetch the global address, then only add the address if it NOT in the local subnet
            elif not get_subnet_ip and extra_ip_addr not in local_subnet:
                host_ips.append(str(extra_ip))

        if host_ips:
            return host_ips[0]

        role_ips = [str(ip) for ip in self.node_ips.get(role, [])]
        if role_ips:
            return role_ips[0]

        raise ValueError(f"Could get prod ip for '{host.address}'")

    def get_default_gateway(self, address):
        # get the default gateway from the node via ssh
        # could just hardcode these values based on the info on the website...
        result = subprocess.run(
            ["ssh", address, "ip -4 route show default"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise RuntimeError(
                f"No default route on {address}: {result.stderr.strip()}"
            )

        out = result.stdout.strip().splitlines()[0].split()
        if "via" not in out:
            raise RuntimeError(f"No gateway used in the default route: {address}")

        return out[out.index("via") + 1]

    def pick_loopback(self, subnet_obj, reserved):
        # since the booked subnets are /22, we get 3 different /24 subnets, so we just make sure that the routers have a loopback address in a
        # /24 subnet that we wont pick for the clients, just to be safe
        subnet = self.parse_subnet(subnet_obj)
        for ip_int in range(
            int(subnet.broadcast_address) - 1, int(subnet.network_address), -1
        ):
            candidate = str(ip_address(ip_int))
            if candidate not in reserved:
                return candidate
        raise ValueError(f"No free loopback in {subnet}")

    def frrouting_setup(self):
        # router mapping: (role, subnet_key) for every router is built dynamically
        ROUTER_MAPPING: list[tuple[str, str]] = [
            ("router_server", "subnet_server"),
        ]
        for i in range(len(self.topology.client_clusters)):
            ROUTER_MAPPING.append((f"router_client_{i}", f"subnet_client_{i}"))

        TEMPLATE_FILE = self.topology.router_template
        DAEMONS_FILE = "./daemons"
        OUTPUT_DIR = Path("./generated_frr_configs")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        with open(TEMPLATE_FILE, "r", encoding="utf-8") as f:
            template = Template(f.read())

        reserved_ips = {str(ip) for ips in self.node_ips.values() for ip in ips}

        for idx, (role, subnet_key) in enumerate(ROUTER_MAPPING):

            if not (subnet_key in self.networks and self.networks[subnet_key]):
                raise RuntimeError(f"Missing subnet {subnet_key}")

            host = self.roles[role][0]
            iface = self.prod_interfaces_per_node[host.address]
            subnet_obj = self.networks[subnet_key][0]

            prod_ip = self.get_router_subnet_or_global_ip(host, role, True)
            global_ip = self.get_router_subnet_or_global_ip(host, role, False)
            net_addr = str(self.parse_subnet(subnet_obj).network_address)
            lo_addr = self.pick_loopback(subnet_obj, reserved_ips)
            reserved_ips.add(lo_addr)
            gateway = self.get_default_gateway(host.address)

            router_id = idx + 1
            is_server = role == "router_server"

            # REMINDER: highest bsr priority wins
            # but lowest rp priority wins
            bsr_prio = router_id + 100 if is_server else router_id
            rp_prio = 0 if is_server else router_id + 100

            tunnels = [
                {"iface": t["iface"], "ip": t["ip"], "network": t["network"]}
                for t in self.router_tunnels.get(role, [])
            ]

            config = template.render(
                lo_address=lo_addr,
                prod_iface=iface,
                prod_net_ip=prod_ip,
                global_ip=global_ip,
                prod_network=net_addr,
                tunnels=tunnels,
                router_id=f"{router_id}.{router_id}.{router_id}.{router_id}",
                isis_router_id=router_id + 1,
                rp_prio=rp_prio,
                bsr_prio=bsr_prio,
                gateway=gateway,
            )

            # write locally and upload file to remote
            local_path = OUTPUT_DIR / f"{host.address.replace('/', '_')}.frr.conf"
            local_path.write_text(config, encoding="utf-8")

            remote = f"root@{host.address}:/etc/frr"
            subprocess.run(["scp", str(local_path), f"{remote}/frr.conf"], check=True)
            subprocess.run(["scp", DAEMONS_FILE, f"{remote}/daemons"], check=True)
            en.run_command(
                "sudo systemctl restart frr",
                task_name=f"restart_frr_{host.address}",
                roles=host,
                gather_facts=False,
            )

            print(
                f"[{role}] {host.address}  prod={prod_ip}  loopback={lo_addr}  gateway={gateway}  tunnels={len(tunnels)}"
            )

    def get_global_ip_for_router_role(self, router_role) -> str:
        local_subnet = ip_network(
            "10.0.0.0/8"
        )  # the subnets we get are in 10..../8, can't be more specific than that sadly
        # since we fetched the global address for the routers and assigned them a local address
        # and we want the global address, we filter out the local address

        host_ips = [
            str(ip)
            for ip in self.node_ips[router_role]
            if ip_address(ip) not in local_subnet
        ]
        if host_ips:
            return host_ips[0]
        raise RuntimeError(f"Could get prod ip for {router_role}")

    def set_default_route_on_host(self, host_alias, cmd, gateway_ip, host_iface):
        result = subprocess.run(
            ["ssh", host_alias, cmd],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"Error on {host_alias}: {result.stderr.strip()}")
        else:
            out = result.stdout.strip().splitlines()
            if out:
                print(out[0])
            print(f"{host_alias}'s default route is {gateway_ip} on {host_iface}")

    def setup_default_routes(self):
        # figure out the gateway router for the server, each client, and each relay
        gateway_router_role = {"server": "router_server"}
        for i in range(len(self.topology.client_clusters)):
            gateway_router_role[f"client_{i}"] = f"router_client_{i}"
            gateway_router_role[f"relay_{i}"] = f"router_client_{i}"

        # Collect all tasks (host_alias, command, gateway_ip, iface) first, then run in parallel
        route_tasks = []

        for role, router_role in gateway_router_role.items():

            # skip any bad role written above
            if role not in self.roles or not self.roles[role]:
                print(f"Unknown role: {role}")
                continue

            gateway_ip = self.get_global_ip_for_router_role(router_role)
            print(gateway_ip)

            for host in self.roles[role]:
                host_iface = self.prod_interfaces_per_node.get(host.address)
                if host_iface is None:
                    raise RuntimeError(
                        f"Missing prod interface for node '{host.address}'"
                    )

                cmd = "; ".join(
                    [
                        f"sudo ip route replace default via {gateway_ip} dev {host_iface}",
                        "sudo ip route flush cache",
                        "ip route show default",
                    ]
                )

                route_tasks.append((host.alias, cmd, gateway_ip, host_iface))

        print(f"setting default routes on {len(route_tasks)} nodes")
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            pool.map(lambda t: self.set_default_route_on_host(*t), route_tasks)

    def push_binaries(
        self,
        bin_dir: str | Path,
        cert_dir: str | Path,
        *,
        remote_bin_dir: str = "/tmp/bin",
        remote_cert_dir: str = "/tmp",
        remote_log_root: str = "/tmp/logs",
        server_binaries: Sequence[str] = ("server", "client"),
        relay_binaries: Sequence[str] = ("fcquic_relay", "app_relay"),
        certs: Sequence[str] = ("cert.crt", "cert.key"),
        binary_mode: str = "0755",
        cert_mode: str = "0644",
    ) -> None:
        """
        pushes the client, server, and relay binaries to the nodes.
        The server and client binaries are sent to every nodes (clients and server).
        The relay binary is sent only to the relay nodes if relays are enabled.
        certs are sent to every node, and the log dirs are setup
        """
        bin_dir = Path(bin_dir)
        cert_dir = Path(cert_dir)

        # look for the files and certs, error out if we can't find them
        for name in [*server_binaries, *relay_binaries]:
            if not (bin_dir / name).is_file():
                raise FileNotFoundError(f"{name} not found in {bin_dir}")
        for name in certs:
            if not (cert_dir / name).is_file():
                raise FileNotFoundError(f"'{name}' not found in {cert_dir}")

        server_and_clients = self.roles["server"] + self.roles["client"]

        # relays are reserved as relay_0, relay_1, ... (one per client cluster)
        relay_hosts = [
            host
            for i in range(len(self.topology.client_clusters))
            for host in (self.roles[f"relay_{i}"] if self.topology.relay_nodes else [])
        ]

        def push(hosts, binaries: Sequence[str], log_subdirs: Sequence[str]):
            with en.actions(roles=hosts) as a:
                a.file(path=remote_bin_dir, state="directory", mode="0755")
                for subdir in log_subdirs:
                    a.file(
                        path=f"{remote_log_root}/{subdir}",
                        state="directory",
                        mode="0755",
                    )
                for name in binaries:
                    a.copy(
                        src=str(bin_dir / name),
                        dest=f"{remote_bin_dir}/{name}",
                        mode=binary_mode,
                    )
                for name in certs:
                    a.copy(
                        src=str(cert_dir / name),
                        dest=f"{remote_cert_dir}/{name}",
                        mode=cert_mode,
                    )

        push(server_and_clients, server_binaries, ["server", "client"])
        print(
            f"Pushed {list(server_binaries)} to {len(server_and_clients)} server/client node(s)"
        )

        if relay_hosts:
            push(relay_hosts, relay_binaries, ["relay"])
            print(f"Pushed {list(relay_binaries)} to {len(relay_hosts)} relay node(s)")

    def stop_reservation(self):
        if self.current_provider is not None:
            self.current_provider.destroy()
            print("Reservation stopped.")
        else:
            print("No reservation ongoing, nothing to stop")
