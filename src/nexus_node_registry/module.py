'''Nexus Node Registry — discovery, config, lifecycle management.'''
import time, random, hashlib
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
from enum import IntEnum

class NodeState(IntEnum):
    UNKNOWN = 0; DISCOVERED = 1; ACTIVE = 2; DEGRADED = 3
    UNRESPONSIVE = 4; OFFLINE = 5; MAINTENANCE = 6

@dataclass
class Node:
    node_id: str; address: str; port: int; capabilities: List[str] = field(default_factory=list)
    state: NodeState = NodeState.UNKNOWN; last_heartbeat: float = 0
    metadata: Dict = field(default_factory=dict)
    version: str = "0.0.1"; uptime_s: float = 0

class NodeRegistry:
    def __init__(self, heartbeat_timeout_s: float = 30):
        self.nodes: Dict[str, Node] = {}; self.timeout = heartbeat_timeout_s
    def register(self, node: Node) -> None:
        node.last_heartbeat = time.time(); node.state = NodeState.ACTIVE
        self.nodes[node.node_id] = node
    def heartbeat(self, node_id: str) -> bool:
        node = self.nodes.get(node_id)
        if not node: return False
        node.last_heartbeat = time.time()
        if node.state in (NodeState.UNKNOWN, NodeState.UNRESPONSIVE):
            node.state = NodeState.ACTIVE
        return True
    def discover(self, capability: str = None) -> List[Node]:
        now = time.time()
        active = [n for n in self.nodes.values()
                 if n.state in (NodeState.ACTIVE, NodeState.DEGRADED)
                 and now - n.last_heartbeat < self.timeout]
        if capability:
            active = [n for n in active if capability in n.capabilities]
        return active
    def check_health(self) -> List[Node]:
        stale = []
        now = time.time()
        for node in self.nodes.values():
            if now - node.last_heartbeat > self.timeout and node.state == NodeState.ACTIVE:
                node.state = NodeState.UNRESPONSIVE; stale.append(node)
        return stale

class ConfigManager:
    def __init__(self):
        self.configs: Dict[str, Dict] = {}; self.version = 0
    def set(self, node_id: str, key: str, value) -> None:
        if node_id not in self.configs: self.configs[node_id] = {}
        self.configs[node_id][key] = value; self.version += 1
    def get(self, node_id: str, key: str, default=None):
        return self.configs.get(node_id, {}).get(key, default)
    def get_all(self, node_id: str) -> Dict:
        return dict(self.configs.get(node_id, {}))

class LifecycleManager:
    def __init__(self, registry: NodeRegistry):
        self.registry = registry
    def activate(self, node_id: str) -> bool:
        node = self.registry.nodes.get(node_id)
        if node: node.state = NodeState.ACTIVE; return True
        return False
    def deactivate(self, node_id: str) -> bool:
        node = self.registry.nodes.get(node_id)
        if node: node.state = NodeState.OFFLINE; return True
        return False
    def maintenance(self, node_id: str) -> bool:
        node = self.registry.nodes.get(node_id)
        if node: node.state = NodeState.MAINTENANCE; return True
        return False

def demo():
    print("=== Node Registry ===")
    reg = NodeRegistry(heartbeat_timeout_s=5)
    for i in range(5):
        reg.register(Node(f"node_{i}", f"192.168.1.{100+i}", 8000+i,
                         ["survey","nav"] if i < 3 else ["comms"]))
    active = reg.discover("survey")
    print(f"  Active survey nodes: {[n.node_id for n in active]}")
    stale = reg.check_health()
    print(f"  Stale nodes: {len(stale)}")
    config = ConfigManager()
    config.set("node_0", "max_depth", 100); config.set("node_0", "sampling_rate", 10)
    print(f"  node_0 config: {config.get_all('node_0')}")
    lifecycle = LifecycleManager(reg)
    lifecycle.maintenance("node_3")
    survey = reg.discover("survey")
    print(f"  Survey after maintenance: {[n.node_id for n in survey]}")

if __name__ == "__main__": demo()
