package redix

import (
	"net"
	"sync"
)

// Wraps a net.Conn
type Conn struct {
	net.Conn

	id  int
	mgr *ConnectionManager
}

// Closes and removes itself from the ConnectionManager
func (conn *Conn) Close() error {
	conn.mgr.mu.Lock()
	defer conn.mgr.mu.Unlock()

	delete(conn.mgr.conns, conn.id)
	return conn.Conn.Close()
}

// ConnectionManager keeps track of connections in
// order to close them as a batch. Instances of net.Conn
// returned by this type remove themselves from the manager
// upon invocations of Close
type ConnectionManager struct {
	mu    sync.RWMutex
	curr  int
	conns map[int]net.Conn
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{conns: map[int]net.Conn{}}
}

// Returns the unique id of the connection
func (mgr *ConnectionManager) Add(conn net.Conn) net.Conn {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.curr++
	id := mgr.curr
	mgr.conns[id] = conn

	return &Conn{Conn: conn, id: id, mgr: mgr}
}

func (mgr *ConnectionManager) CloseAll() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for curr, conn := range mgr.conns {
		delete(mgr.conns, curr)
		conn.Close()
	}
}
