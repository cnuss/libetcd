package v1alpha1

import v1 "github.com/cnuss/libetcd/v1"

// WithName sets the node (member) name.
func (b *BuilderImpl) WithName(name string) v1.Builder {
	b.name = name
	return b
}

// WithDir sets the data directory.
func (b *BuilderImpl) WithDir(dir string) v1.Builder {
	b.dir = dir
	return b
}

// WithClientPort sets the localhost client port (0 = pick a free port at Start).
func (b *BuilderImpl) WithClientPort(port int) v1.Builder {
	b.clientPort = &port
	return b
}

// WithPeerPort sets the localhost peer port (0 = pick a free port at Start).
func (b *BuilderImpl) WithPeerPort(port int) v1.Builder {
	b.peerPort = &port
	return b
}

// WithClientURL sets explicit listen+advertise client URLs, overriding the port.
func (b *BuilderImpl) WithClientURL(urls ...string) v1.Builder {
	b.clientURLs = urls
	return b
}

// WithPeerURL sets explicit listen+advertise peer URLs, overriding the port.
func (b *BuilderImpl) WithPeerURL(urls ...string) v1.Builder {
	b.peerURLs = urls
	return b
}

// WithPeers declares a multi-node initial cluster (member name -> peer URL).
func (b *BuilderImpl) WithPeers(peers map[string]string) v1.Builder {
	b.peers = peers
	return b
}

// WithClusterToken sets the initial-cluster token.
func (b *BuilderImpl) WithClusterToken(token string) v1.Builder {
	b.token = token
	return b
}

// WithExistingCluster marks the node as joining an existing cluster.
func (b *BuilderImpl) WithExistingCluster() v1.Builder {
	b.existing = true
	return b
}

// WithLogLevel sets the server log level.
func (b *BuilderImpl) WithLogLevel(level string) v1.Builder {
	b.logLevel = level
	return b
}
