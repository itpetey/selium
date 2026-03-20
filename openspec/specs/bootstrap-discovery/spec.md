# Bootstrap Discovery

Mechanism for nodes to discover peers and join the cluster at startup.

## Requirements

### Requirement: Nodes discover peers via DNS TXT record
New nodes SHALL discover existing cluster nodes by querying a DNS TXT record.

#### Scenario: Query TXT record on startup
- **WHEN** a node starts
- **THEN** it SHALL query the DNS TXT record for seed addresses
- **AND** it SHALL use the returned addresses to join the cluster

### Requirement: First node becomes seed and populates TXT
When a node starts with an empty TXT record, it SHALL become the seed and populate the TXT record.

#### Scenario: First node populates TXT
- **WHEN** a node starts and the TXT record is empty
- **THEN** the node SHALL become the seed
- **AND** SHALL populate the TXT record with its own address

### Requirement: Gossip propagates node information
Nodes SHALL use a gossip protocol to disseminate node membership information.

#### Scenario: Gossip new node to cluster
- **WHEN** a new node joins the cluster
- **THEN** it SHALL announce itself to existing nodes via gossip
- **AND** existing nodes SHALL propagate the announcement to their neighbors

### Requirement: Nodes refresh TXT record periodically
Seed nodes SHALL periodically refresh the DNS TXT record with current cluster membership.

#### Scenario: Periodic TXT refresh
- **WHEN** a seed node's cluster membership changes
- **THEN** the seed SHALL update the TXT record within a configurable interval

### Requirement: Failed nodes are removed from TXT
Failed nodes SHALL be removed from the TXT record to prevent new nodes from trying to join dead nodes.

#### Scenario: Remove failed node from TXT
- **WHEN** a node fails to respond to gossip probes
- **THEN** the seed SHALL remove it from the TXT record
- **AND** gossip SHALL propagate the removal