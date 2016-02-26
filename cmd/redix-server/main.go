package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/kevin-cantwell/redix"

	"golang.org/x/net/context"
)

// TODOs:
// Use net.ResolveTCPAddr to resolve masters/slaves before opening

func main() {
	port := os.Getenv("PORT")
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on " + ":" + port)

	promoting := make(chan struct{})
	promoted := make(chan string)
	serverURL := os.Getenv("REDIS_URL") // TODO: source this from writable config file
	ctx, cancel := context.WithCancel(context.Background())
	for {
		// Listen for an incoming connection.
		clientConn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting client connection: ", err.Error())
			continue
		}

		select {
		// If a promotion is in progress:
		// Kill existing connections, block new connections, and update the serverURL
		case <-promoting:
			// Kill existing connections
			cancel()
			// Block new connections, update the serverURL
			serverURL = <-promoted
			ctx, cancel = context.WithCancel(context.Background())
		default:
			proxy := redix.NewProxy(clientConn, redix.ProxyConfig{Promoting: promoting, Promoted: promoted})
			go handleProxy(ctx, proxy, serverURL)
		}
	}
}

func handleProxy(ctx context.Context, proxy redix.Proxy, serverURL string) {
	defer proxy.Close()

	if err := proxy.Open(serverURL); err != nil {
		proxy.WriteClientErr(err)
		return
	}

	// Handle client requests, but die if kill signal is sent.
	for {
		select {
		// Kill signal received
		case <-ctx.Done():
			return
		// Client sends a request
		case clientResp := <-proxy.ReadClientObject():
			// Handle client errors
			if clientResp.Err != nil {
				if clientResp.Err == io.EOF {
					proxy.Println("client sent EOF")
					return
				}
				if err := proxy.WriteClientErr(clientResp.Err); err != nil {
					return
				}
				continue
			}

			// Intercept any extended commands, such as PROMOTE
			if strings.ToUpper(string(clientResp.Body)) == "*2\r\n$7\r\nPROMOTE\r\n$6\r\nSLAVE0\r\n" {
				proxy.Println("promoting slave0")
				if err := proxy.Promote("slave0"); err != nil {
					if err := proxy.WriteClientErr(err); err != nil {
						return
					}
				}
				continue
			}
			// case "*1\r\n$7\r\nMONITOR\r\n":
			// 	monitor = true
			// }

			select {
			// Kill signal received
			case <-ctx.Done():
				return
			// Forward client request to server and wait for response
			case serverResp := <-proxy.WriteServerObject(clientResp.Body):
				// Handle server errors
				if serverResp.Err != nil {
					if serverResp.Err == io.EOF {
						proxy.Println("server sent EOF")
						return
					}
					if err := proxy.WriteClientErr(serverResp.Err); err != nil {
						return
					}
					continue
				}

				if err := proxy.WriteClientObject(serverResp.Body); err != nil {
					return
				}
			}
		}
	}
}

// // Proxies clients to a backend redis server. Intercepts certain commands
// // for special handling.
// func proxyConnections(proxy redix.RedisProxy) {
// 	defer proxy.Close()

// 	for {
// 		var monitor bool

// 		// Read the next command
// 		request, err := proxy.ReadClientObject()
// 		if err != nil {
// 			if err == io.EOF {
// 				proxy.Println("client sent EOF")
// 				return
// 			}
// 			proxy.WriteClientErr(err)
// 			continue
// 		}

// 		// Intercept any extended commands, such as PROMOTE
// 		switch strings.ToUpper(string(request)) {
// 		case "*3\r\n$7\r\nPROMOTE\r\n$9\r\nLOCALHOST\r\n$4\r\n6380\r\n":
// 			if err := proxy.Promote("localhost:6380"); err != nil {
// 				if err == io.EOF {
// 					proxy.Println("server sent EOF")
// 					return
// 				}
// 				proxy.WriteClientErr(err)
// 			}
// 			continue
// 		case "*1\r\n$7\r\nMONITOR\r\n":
// 			monitor = true
// 		}

// 		// Proxy the client request to the server
// 		if err := proxy.WriteServerObject(request); err != nil {
// 			proxy.WriteClientErr(err)
// 			continue
// 		}

// 		response, err := proxy.ReadServerObject()
// 		if err != nil {
// 			if err == io.EOF {
// 				proxy.Println("server sent EOF")
// 				return
// 			}
// 			proxy.WriteClientErr(err)
// 			continue
// 		}

// 		// Proxy the server response back to the client
// 		proxy.WriteClientObject(response)

// 		// If monitoring was invoked, copy the byte streams back and forth.
// 		if monitor {
// 			proxy.Monitor()
// 			return
// 		}
// 	}
// }

// func promote(clientConn, serverConn net.Conn, masterReader *RESPReader) (newMaster net.Conn, err error) {
// 	slaveURL := "localhost:6380" // TODO: source this from env vars???
// 	slaveConn, err := net.Dial("tcp", slaveURL)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer func() {
// 		if err != nil {
// 			slaveConn.Close()
// 		}
// 	}()
// 	slaveReader := NewReader(slaveConn)

// 	// ClientWrite(clientConn, slaveConn, "$127\r\n(promote) Pausing all incoming requests...\r\n")
// 	// Pause all incoming requests
// 	masterWriteLock.Lock()
// 	defer masterWriteLock.Unlock()

// 	// ClientWrite(clientConn, slaveConn, "(promote) Waiting for sync to complete...\r\n")
// 	if err := ServerWrite(slaveConn, "*1\r\n$4\r\nINFO\r\n"); err != nil {
// 		return nil, err
// 	}

// 	// Send updates back to the client
// 	response, err := slaveReader.ReadObject()
// 	if err != nil {
// 		return nil, err
// 	}
// 	// ClientWrite(clientConn, slaveConn, "(promote) TODO: Parse master_repl_offset\r\n")
// 	// ClientWrite(clientConn, slaveConn, string(response))
// 	response = response
// 	ClientWrite(clientConn, slaveConn, "$2288\r\n# Server\r\nredis_version:3.0.7\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:ca156bbb7812e807\r\nredis_mode:standalone\r\nos:Darwin 14.5.0 x86_64\r\narch_bits:64\r\nmultiplexing_api:kqueue\r\ngcc_version:4.2.1\r\nprocess_id:23613\r\nrun_id:222f14101514a2872a6ff57f3532ed56b04d9bdb\r\ntcp_port:6380\r\nuptime_in_seconds:83810\r\nuptime_in_days:0\r\nhz:10\r\nlru_clock:13599839\r\nconfig_file:\r\n\r\n# Clients\r\nconnected_clients:4\r\nclient_longest_output_list:0\r\nclient_biggest_input_buf:0\r\nblocked_clients:0\r\n\r\n# Memory\r\nused_memory:1066528\r\nused_memory_human:1.02M\r\nused_memory_rss:872448\r\nused_memory_peak:1066528\r\nused_memory_peak_human:1.02M\r\nused_memory_lua:36864\r\nmem_fragmentation_ratio:0.82\r\nmem_allocator:libc\r\n\r\n# Persistence\r\nloading:0\r\nrdb_changes_since_last_save:6\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:1456356605\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:-1\r\nrdb_current_bgsave_time_sec:-1\r\naof_enabled:0\r\naof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:ok\r\naof_last_write_status:ok\r\n\r\n# Stats\r\ntotal_connections_received:12\r\ntotal_commands_processed:998\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:16450\r\ntotal_net_output_bytes:634722\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.04\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nevicted_keys:0\r\nkeyspace_hits:3\r\nkeyspace_misses:1\r\npubsub_channels:0\r\npubsub_patterns:0\r\nlatest_fork_usec:0\r\nmigrate_cached_sockets:0\r\n\r\n# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:3\r\nmaster_sync_in_progress:0\r\nslave_repl_offset:13516\r\nslave_priority:100\r\nslave_read_only:1\r\nconnected_slaves:0\r\nmaster_repl_offset:0\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n\r\n# CPU\r\nused_cpu_sys:10.94\r\nused_cpu_user:4.13\r\nused_cpu_sys_children:0.00\r\nused_cpu_user_children:0.00\r\n\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n# Keyspace\r\ndb0:keys=2,expires=0,avg_ttl=0\r\ndb1:keys=2,expires=0,avg_ttl=0\r\ndb3:keys=1,expires=0,avg_ttl=0\r\ndb4:keys=3,expires=0,avg_ttl=0\r\ndb10:keys=4,expires=0,avg_ttl=0\r\ndb11:keys=17,expires=0,avg_ttl=0\r\ndb12:keys=1,expires=0,avg_ttl=0\r\n\r\n")
// 	ClientWrite(clientConn, serverConn, "$2288\r\n# Server\r\nredis_version:3.0.7\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:ca156bbb7812e807\r\nredis_mode:standalone\r\nos:Darwin 14.5.0 x86_64\r\narch_bits:64\r\nmultiplexing_api:kqueue\r\ngcc_version:4.2.1\r\nprocess_id:23613\r\nrun_id:222f14101514a2872a6ff57f3532ed56b04d9bdb\r\ntcp_port:6380\r\nuptime_in_seconds:83810\r\nuptime_in_days:0\r\nhz:10\r\nlru_clock:13599839\r\nconfig_file:\r\n\r\n# Clients\r\nconnected_clients:4\r\nclient_longest_output_list:0\r\nclient_biggest_input_buf:0\r\nblocked_clients:0\r\n\r\n# Memory\r\nused_memory:1066528\r\nused_memory_human:1.02M\r\nused_memory_rss:872448\r\nused_memory_peak:1066528\r\nused_memory_peak_human:1.02M\r\nused_memory_lua:36864\r\nmem_fragmentation_ratio:0.82\r\nmem_allocator:libc\r\n\r\n# Persistence\r\nloading:0\r\nrdb_changes_since_last_save:6\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:1456356605\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:-1\r\nrdb_current_bgsave_time_sec:-1\r\naof_enabled:0\r\naof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:ok\r\naof_last_write_status:ok\r\n\r\n# Stats\r\ntotal_connections_received:12\r\ntotal_commands_processed:998\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:16450\r\ntotal_net_output_bytes:634722\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.04\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nevicted_keys:0\r\nkeyspace_hits:3\r\nkeyspace_misses:1\r\npubsub_channels:0\r\npubsub_patterns:0\r\nlatest_fork_usec:0\r\nmigrate_cached_sockets:0\r\n\r\n# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:3\r\nmaster_sync_in_progress:0\r\nslave_repl_offset:13516\r\nslave_priority:100\r\nslave_read_only:1\r\nconnected_slaves:0\r\nmaster_repl_offset:0\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n\r\n# CPU\r\nused_cpu_sys:10.94\r\nused_cpu_user:4.13\r\nused_cpu_sys_children:0.00\r\nused_cpu_user_children:0.00\r\n\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n# Keyspace\r\ndb0:keys=2,expires=0,avg_ttl=0\r\ndb1:keys=2,expires=0,avg_ttl=0\r\ndb3:keys=1,expires=0,avg_ttl=0\r\ndb4:keys=3,expires=0,avg_ttl=0\r\ndb10:keys=4,expires=0,avg_ttl=0\r\ndb11:keys=17,expires=0,avg_ttl=0\r\ndb12:keys=1,expires=0,avg_ttl=0\r\n\r\n")
// 	slaveConn.Close()

// 	return serverConn, nil
// }

// func ProxyPrintln(clientConn, serverConn net.Conn, args ...interface{}) {
// 	allArgs := make([]interface{}, len(args)+1)
// 	allArgs[0] = clientConn.RemoteAddr().String() + " <> " + serverConn.RemoteAddr().String() + ":"
// 	for i, arg := range args {
// 		allArgs[i+1] = arg
// 	}
// 	fmt.Println(allArgs...)
// }

// func ClientWrite(clientConn, serverConn net.Conn, resp string) error {
// 	serverAddr := "127.0.0.1:" + os.Getenv("PORT")
// 	if serverConn != nil {
// 		serverAddr = serverConn.RemoteAddr().String()
// 	}
// 	fmt.Printf("%s <- %s:\n%s\n", clientConn.RemoteAddr().String(), serverAddr, resp)
// 	_, err := clientConn.Write([]byte(resp))
// 	return err
// }

// func MasterWrite(clientConn, serverConn net.Conn, resp string) error {
// 	clientAddr := "127.0.0.1:" + os.Getenv("PORT")
// 	if clientConn != nil {
// 		clientAddr = clientConn.RemoteAddr().String()
// 	}
// 	fmt.Printf("%s -> %s: %q\n", clientAddr, serverConn.RemoteAddr().String(), resp)
// 	masterWriteLock.Lock()
// 	_, err := serverConn.Write([]byte(resp))
// 	masterWriteLock.Unlock()
// 	return err
// }

// func ServerWrite(serverConn net.Conn, resp string) error {
// 	fmt.Printf("%s -> %s: %q\n", "127.0.0.1:"+os.Getenv("PORT"), serverConn.RemoteAddr().String(), resp)
// 	_, err := serverConn.Write([]byte(resp))
// 	return err
// }
