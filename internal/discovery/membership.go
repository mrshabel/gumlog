package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// cluster membership definition for service discovery
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	// entry and exist events channel
	events chan serf.Event
	// logger instance for service discovery activities
	logger *zap.Logger
}

// New creates a new serf membership instance for the current node
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// configuration for current serf node
type Config struct {
	// unique name of the current node. defaults to its hostname
	NodeName string
	// address for gossiping
	BindAddr string

	// key value metadata tags to give more context about the node.
	// can be used to shared info on whether a node is a voter or not,
	// and RPC addresses
	Tags map[string]string
	// existing node addresses that any new node can join. the new node
	// will connect to one node in the defined addresses and then broadcast
	// its presence to the other nodes through gossiping
	StartJoinAddrs []string
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()

	// include current node membership details for gossiping
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	m.events = make(chan serf.Event)
	config.EventCh = m.events

	// key value metadata tags
	config.Tags = m.Tags
	config.NodeName = m.NodeName

	// create service discovery instance
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	// handle events
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		// join an existing cluster
		if _, err = m.serf.Join(m.StartJoinAddrs, true); err != nil {
			return err
		}
	}
	return nil
}

// Handler represents a component in the service that needs to know
// when a server joins or leaves the cluster
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// eventHandler handles Join and Leave events for its members. it runs in an
// endless loop to ensure that all events are delivered.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			// broadcast event to all members. the current event may contain
			// one or more members
			for _, member := range e.(serf.MemberEvent).Members {
				// skip broadcasting event to itself
				if !m.isLocal(member) {
					m.handleJoin(member)
				}
			}
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				// skip broadcasting event to itself
				if !m.isLocal(member) {
					m.handleLeave(member)
				}
			}
		}
	}
}

// handleJoins adds a new member to the cluster with their names and
// rpc address tags
func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// handleJoins removes a member from the cluster with their name
func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal checks whether the given member is the current local node
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members return a snapshot of  all the current members in the cluster
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave tells member to leave the cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError logs the given error message with the member's details
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg, zap.Error(err), zap.String("name", member.Name), zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
