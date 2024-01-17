package pgx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"time"
)

const NO_SERVERS_MSG = "could not find a server to connect to"
const MAX_RETRIES = 20
const REFRESH_INTERVAL_SECONDS = 300
const DEFAULT_FAILED_HOST_RECONNECT_DELAY_SECS = 5
const MAX_FAILED_HOST_RECONNECT_DELAY_SECS = 60
const MAX_INTERVAL_SECONDS = 600
const MAX_PREFERENCE_VALUE = 10

var ErrFallbackToOriginalBehaviour = errors.New("no preferred server available, fallback-to-topology-keys-only is set to true so falling back to original behaviour")

// -- Values for ClusterLoadInfo.flags --
// Use private address (host) of tservers to create a connection
const USE_HOSTS byte = 0

// Use public address (public_ip) of tservers to create a connection
const USE_PUBLIC_IP byte = 1

// Try both the addresses (host, public_ip) of tservers to create a connection
const TRY_HOSTS_PUBLIC_IP byte = 2

// Both the addresses (host, public_ip) of tserver to be tried, but no success with private addresses to create a connection
const HOSTS_EXHAUSTED byte = 3

// Indicate to the Go routine processing the requestChan that it should return a least loaded tserver host, port
const GET_LB_CONN byte = 4

// Indicate to the Go routine processing the requestChan that it should decrease the connection count for the given host by one
const DECREMENT_COUNT byte = 5

type ClusterLoadInfo struct {
	clusterName string
	ctx         context.Context
	config      *ConnConfig
	controlConn *Conn
	ctrlCtx     context.Context
	lastRefresh time.Time
	// map of host -> connection count
	hostLoad map[string]int
	// map of host -> port
	hostPort map[string]uint16
	// map of "cloud.region.zone" -> slice of hostnames
	zoneList map[string][]string
	// map of host -> int
	unavailableHosts map[string]int64
	// map of (private -> public) address of a node.
	hostPairs map[string]string
	flags     byte
}

type lbHost struct {
	hostname string
	port     uint16
	err      error
}

var clustersLoadInfo map[string]*ClusterLoadInfo

const LB_QUERY = "SELECT host,port,num_connections,node_type,cloud,region,zone,public_ip FROM yb_servers()"

// Only the Go routine spawned in init() reads this channel. Based on the flag, it
// - returns the least loaded tserver's host/port (GET_LB_CONN)
// - decrements connection count by one for closed connection (DECREMENT_COUNT)
var requestChan chan *ClusterLoadInfo

// Only the Go routine spawned in init() writes to this channel.
// It returns the least loaded tserver's host/port if successful else err
var hostChan chan *lbHost

func NewClusterLoadInfo(ctx context.Context, config *ConnConfig) *ClusterLoadInfo {
	info := new(ClusterLoadInfo)
	info.clusterName = LookupIP(config.Host)
	info.ctx = ctx
	info.config = config
	info.flags = GET_LB_CONN
	return info
}

func LookupIP(host string) string {
	addrs, err := net.LookupHost(host)
	if err == nil {
		for _, addr := range addrs {
			if strings.Contains(addr, ".") {
				return addr
			}
		}
		if len(addrs) > 0 {
			return addrs[0]
		}
	}
	return host
}

func init() {
	clustersLoadInfo = make(map[string]*ClusterLoadInfo)
	requestChan = make(chan *ClusterLoadInfo)
	hostChan = make(chan *lbHost)
	go produceHostName(requestChan, hostChan)
}

func replaceHostString(connString string, newHost string, port uint16) string {
	newConnString := connString
	if strings.HasPrefix(connString, "postgres://") || strings.HasPrefix(connString, "postgresql://") {
		if strings.Contains(connString, "@") {
			pattern := regexp.MustCompile("@([^/]*)/")
			// todo IPv6 handling
			newConnString = pattern.ReplaceAllString(connString, fmt.Sprintf("@%s:%d/", newHost, port))
		} else {
			pattern := regexp.MustCompile("://([^/]*)/")
			newConnString = pattern.ReplaceAllString(connString, fmt.Sprintf("://%s:%d/", newHost, port))
		}
	} else { // key = value (DSN style)
		pattern := regexp.MustCompile("host=([^/]*) ")
		newConnString = pattern.ReplaceAllString(connString, fmt.Sprintf("host=%s ", newHost))
		pattern = regexp.MustCompile("port=([^/]*) ")
		newConnString = pattern.ReplaceAllString(newConnString, fmt.Sprintf("port=%d ", port))
	}
	return newConnString
}

func produceHostName(in chan *ClusterLoadInfo, out chan *lbHost) {

	for {
		new, present := <-in

		if !present {
			log.Println("The requestChannel is closed, load_balance feature will not work")
			break
		}
		if new.flags == DECREMENT_COUNT {
			names := strings.Split(new.clusterName, ",")
			if len(names) != 2 {
				log.Printf("cannot parse names to update connection count: %s", new.clusterName)
			} else {
				cli, ok := clustersLoadInfo[LookupIP(names[0])]
				if ok {
					cnt := cli.hostLoad[names[1]]
					if cnt == 0 {
						log.Printf("connection count for %s going negative!", names[1])
					}
					cli.hostLoad[names[1]] = cnt - 1
				}
			}
			continue
		}
		old, present := clustersLoadInfo[new.clusterName]
		if !present {
			// There is no loadInfo available for this config. Create one.
			err := refreshLoadInfo(new)
			if err != nil {
				lb := &lbHost{
					hostname: "",
					err:      err,
				}
				out <- lb
				continue
			}
			publicIpAvailable := false
			for k, v := range new.hostPairs {
				if v != "" {
					publicIpAvailable = true
				}
				if new.clusterName == k {
					new.flags = USE_HOSTS
					break
				} else if new.clusterName == v {
					new.flags = USE_PUBLIC_IP
					break
				} else {
					new.flags = TRY_HOSTS_PUBLIC_IP
				}
			}
			if !publicIpAvailable {
				new.flags = USE_HOSTS
			}

			clustersLoadInfo[new.clusterName] = new

			out <- getHostWithLeastConns(new)
			// continue
		} else {
			old.config.topologyKeys = new.config.topologyKeys // Use the provided topology-keys.
			old.config.fallbackToTopologyKeysOnly = new.config.fallbackToTopologyKeysOnly
			old.config.failedHostReconnectDelaySecs = new.config.failedHostReconnectDelaySecs
			old.config.connString = new.config.connString
			out <- refreshAndGetLeastLoadedHost(old, new.unavailableHosts)
			// continue
		}
	}
}

func connectLoadBalanced(ctx context.Context, config *ConnConfig) (c *Conn, err error) {
	newLoadInfo := NewClusterLoadInfo(ctx, config)
	requestChan <- newLoadInfo
	lbHost := <-hostChan
	if lbHost.err != nil {
		log.Printf("Could not load balance connections, falling back to upstream behaviour, %s", lbHost.err)
		return connect(ctx, config) // fallback to original behaviour
	}
	if lbHost.hostname == config.Host {
		return connectWithRetries(ctx, config.controlHost, config, newLoadInfo, lbHost)
	} else {
		newConnString := replaceHostString(config.connString, lbHost.hostname, lbHost.port)
		newConfig, err := ParseConfig(newConnString)
		if err != nil {
			return nil, err
		}
		newConfig.Port = lbHost.port
		newConfig.controlHost = config.controlHost
		newConfig.TLSConfig = config.TLSConfig
		return connectWithRetries(ctx, config.controlHost, newConfig, newLoadInfo, lbHost)
	}
}

func connectWithRetries(ctx context.Context, controlHost string, newConfig *ConnConfig,
	newLoadInfo *ClusterLoadInfo, lbHost *lbHost) (c *Conn, er error) {
	conn, err := connect(ctx, newConfig)
	for i := 0; i < MAX_RETRIES && err != nil; i++ {
		decrementConnCount(newConfig.controlHost + "," + newConfig.Host)
		log.Printf("adding %s to unavailableHosts due to %s", newConfig.Host, err.Error())
		newLoadInfo.unavailableHosts = map[string]int64{lbHost.hostname: time.Now().Unix()}
		requestChan <- newLoadInfo
		lbHost = <-hostChan
		if lbHost.err != nil {
			return nil, lbHost.err
		}
		if lbHost.hostname == newConfig.Host {
			conn, err = connect(context.Background(), newConfig)
		} else {
			newConnString := strings.Replace(newConfig.connString, newConfig.Host, lbHost.hostname, -1)
			oldTLSConfig := newConfig.TLSConfig
			newConfig, err = ParseConfig(newConnString)
			if err != nil {
				return nil, err
			}
			newConfig.Port = lbHost.port
			newConfig.controlHost = controlHost
			newConfig.TLSConfig = oldTLSConfig
			conn, err = connect(context.Background(), newConfig)
		}
	}
	if err != nil {
		decrementConnCount(newConfig.controlHost + "," + newConfig.Host)
	}
	return conn, err
}

func decrementConnCount(str string) {
	requestChan <- &ClusterLoadInfo{
		clusterName: str,
		flags:       DECREMENT_COUNT,
	}
}

func markHostAway(li *ClusterLoadInfo, h string) {
	log.Printf("Marking host %s as unreachable", h)
	delete(li.hostLoad, h)
	delete(li.hostPairs, h)
	if li.unavailableHosts == nil {
		li.unavailableHosts = make(map[string]int64)
	}
	li.unavailableHosts[h] = time.Now().Unix()
}

func refreshLoadInfo(li *ClusterLoadInfo) error {
	li.ctrlCtx, _ = context.WithTimeout(context.Background(), 30*time.Second)
	if li.controlConn == nil || li.controlConn.IsClosed() {
		var err error
		ctrlConfig, err := ParseConfig(li.config.connString)
		if err != nil {
			log.Printf("refreshLoadInfo(): ParseConfig for control connection failed, %s", err.Error())
			return err
		}
		li.config = ctrlConfig
		li.config.ConnectTimeout = 15 * time.Second
		li.controlConn, err = connect(li.ctrlCtx, li.config)
		if err != nil {
			log.Printf("Could not create control connection to %s\n", li.config.Host)
			// remove its hostLoad entry
			markHostAway(li, li.config.Host)
			li.controlConn = nil
			// Attempt connection to other servers which are already fetched in cli.
			if len(li.hostPairs) > 0 {
				log.Printf("Attempting control connection to %d other servers ...\n", len(li.hostPairs))
			}
			for h := range li.hostPairs {
				newConnString := replaceHostString(li.config.connString, h, li.hostPort[h])
				if li.config, err = ParseConfig(newConnString); err == nil {
					li.ctrlCtx, _ = context.WithTimeout(context.Background(), 30*time.Second)
					if li.controlConn, err = connect(li.ctrlCtx, li.config); err == nil {
						log.Printf("Created control connection to host %s", h)
						break
					}
					log.Printf("Could not create control connection to host %s", h)
					markHostAway(li, li.config.Host)
					li.controlConn = nil
				}
			}
			if err != nil {
				log.Printf("Failed to create control connection")
				return err
			}
		}
	}
	// defer li.controlConn.Close(li.ctrlCtx)
	// defer cancel() ?

	li.controlConn.stmtcache.Clear(li.ctrlCtx)
	rows, err := li.controlConn.Query(li.ctrlCtx, LB_QUERY)
	if err != nil {
		log.Printf("Could not query load information: %s", err.Error())
		markHostAway(li, li.config.Host)
		li.controlConn = nil
		return refreshLoadInfo(li)
	}
	defer rows.Close()
	var host, nodeType, cloud, region, zone, publicIP string
	var port, numConns int
	newHostLoad := make(map[string]int)
	li.hostPort = make(map[string]uint16)
	li.zoneList = make(map[string][]string)
	li.hostPairs = make(map[string]string)
	if li.unavailableHosts == nil {
		li.unavailableHosts = make(map[string]int64)
	}
	for rows.Next() {
		err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
		if err != nil {
			log.Printf("Could not read load information: %s", err.Error())
			markHostAway(li, li.config.Host)
			li.controlConn = nil
			return refreshLoadInfo(li)
		} else {
			li.hostPairs[host] = publicIP
			tk := cloud + "." + region + "." + zone
			tk_star := cloud + "." + region // Used for topology_keys of type: cloud.region.*
			hosts, ok := li.zoneList[tk]
			if !ok {
				hosts = make([]string, 0)
			}
			hosts_star, ok_star := li.zoneList[tk_star]
			if !ok_star {
				hosts_star = make([]string, 0)
			}
			hosts = append(hosts, host)
			hosts_star = append(hosts_star, host)
			li.zoneList[tk] = hosts
			li.zoneList[tk_star] = hosts_star
			cnt := li.hostLoad[host]
			newHostLoad[host] = cnt
			li.hostPort[host] = uint16(port)
		}
	}

	rsError := rows.Err()
	if rsError != nil {
		log.Printf("refreshLoadInfo(): Could not read load information, Rows.Err(): %s", rsError.Error())
		markHostAway(li, li.config.Host)
		li.controlConn = nil
		return refreshLoadInfo(li)
	}

	li.hostLoad = newHostLoad
	li.lastRefresh = time.Now()
	for uh, t := range li.unavailableHosts {
		if time.Now().Unix()-t > li.config.failedHostReconnectDelaySecs {
			// clear the unavailable-hosts list
			li.hostLoad[uh] = 0
			delete(li.unavailableHosts, uh)
		}
	}
	return nil
}

func getHostWithLeastConns(li *ClusterLoadInfo) *lbHost {
	leastCnt := int(math.MaxInt64)
	leastLoaded := ""
	leastLoadedservers := make([]string, 0)
	if li.config.topologyKeys != nil {
		for i := 0; i < len(li.config.topologyKeys); i++ {
			var servers []string
			for _, tk := range li.config.topologyKeys[i] {
				toCheckStar := strings.Split(tk, ".")
				if toCheckStar[2] == "*" {
					tk = toCheckStar[0] + "." + toCheckStar[1]
				}
				log.Printf("Including all nodes for tk %s", tk)
				servers = append(servers, li.zoneList[tk]...)
			}
			for _, h := range servers {
				if !isHostAway(li, h) {
					if li.hostLoad[h] < leastCnt {
						leastLoadedservers = nil
						leastLoadedservers = append(leastLoadedservers, h)
						leastCnt = li.hostLoad[h]
					} else if li.hostLoad[h] == leastCnt {
						leastLoadedservers = append(leastLoadedservers, h)
					}
				}
			}
			if leastCnt != int(math.MaxInt64) && len(leastLoadedservers) != 0 {
				break
			} else {
				log.Printf("Did not find any UP nodes in %s", li.config.topologyKeys[i])
			}
		}
	}
	if leastCnt == int(math.MaxInt64) && len(leastLoadedservers) == 0 {
		if li.config.topologyKeys == nil || !li.config.fallbackToTopologyKeysOnly {
			for h := range li.hostLoad {
				if !isHostAway(li, h) {
					if li.hostLoad[h] < leastCnt {
						leastLoadedservers = nil
						leastLoadedservers = append(leastLoadedservers, h)
						leastCnt = li.hostLoad[h]
					} else if li.hostLoad[h] == leastCnt {
						leastLoadedservers = append(leastLoadedservers, h)
					}
				}
			}
		} else {
			lbh := &lbHost{
				err: ErrFallbackToOriginalBehaviour,
			}
			return lbh
		}
	}

	if len(leastLoadedservers) != 0 {
		rand.Seed(time.Now().UnixNano())
		leastLoaded = leastLoadedservers[rand.Intn(len(leastLoadedservers))]
	}

	if leastLoaded == "" {
		if li.flags == TRY_HOSTS_PUBLIC_IP {
			// remove all host (private ips) from unavailable list
			for h := range li.hostPairs {
				delete(li.unavailableHosts, h)
			}
			li.flags = HOSTS_EXHAUSTED
			return getHostWithLeastConns(li)
		}
		lbh := &lbHost{
			hostname: "",
			err:      errors.New(NO_SERVERS_MSG),
		}
		return lbh
	}
	leastLoadedToUse := leastLoaded
	if li.flags == USE_PUBLIC_IP || li.flags == HOSTS_EXHAUSTED {
		leastLoadedToUse = li.hostPairs[leastLoaded]
		if leastLoadedToUse == "" {
			lbh := &lbHost{
				hostname: "",
				err:      errors.New(NO_SERVERS_MSG),
			}
			return lbh
		}
	}
	lbh := &lbHost{
		hostname: leastLoadedToUse,
		port:     li.hostPort[leastLoaded],
		err:      nil,
	}
	li.hostLoad[leastLoadedToUse] = leastCnt + 1
	return lbh
}

func isHostAway(li *ClusterLoadInfo, h string) bool {
	for awayHost := range li.unavailableHosts {
		if h == awayHost || h == li.hostPairs[awayHost] {
			return true
		}
	}
	return false
}

func refreshAndGetLeastLoadedHost(li *ClusterLoadInfo, awayHosts map[string]int64) *lbHost {
	if time.Now().Unix()-li.lastRefresh.Unix() > li.config.refreshInterval {
		err := refreshLoadInfo(li)
		if err != nil {
			return &lbHost{
				hostname: "",
				err:      err,
			}
		}
	}

	for h := range awayHosts {
		li.unavailableHosts[h] = awayHosts[h]
	}
	return getHostWithLeastConns(li)
}

// expects the toplogykeys in the format 'cloud1.region1.zone1,cloud1.region1.zone2,...'
func validateTopologyKeys(s string) ([]string, error) {
	tkeys := strings.Split(s, ",")
	for _, tk := range tkeys {
		zones1 := strings.Split(tk, ".")
		zones2 := strings.Split(tk, ":")
		if len(zones1) != 3 || len(zones2) > 2 {
			return nil, errors.New("toplogy_keys '" + s +
				"' not in correct format, should be specified as '<cloud>.<region>.<zone>,...'")
		}
	}
	return tkeys, nil
}

// For test purpose
func GetHostLoad() map[string]map[string]int {
	hl := make(map[string]map[string]int)
	for cluster := range clustersLoadInfo {
		hl[cluster] = make(map[string]int)
		for host, cnt := range clustersLoadInfo[cluster].hostLoad {
			hl[cluster][host] = cnt
		}
	}
	return hl
}

// For test purpose
func GetAZInfo() map[string]map[string][]string {
	az := make(map[string]map[string][]string)
	for n, cli := range clustersLoadInfo {
		az[n] = make(map[string][]string)
		for z, hosts := range cli.zoneList {
			q := strings.Split(z, ".")
			if len(q) == 3 {
				newzl := make([]string, len(hosts))
				copy(newzl, hosts)
				az[n][z] = newzl
			}
		}
	}
	return az
}

// For test purpose
func EmptyHostLoad() map[string]map[string]int {
	for cluster := range clustersLoadInfo {
		for host := range clustersLoadInfo[cluster].hostLoad {
			delete(clustersLoadInfo[cluster].hostLoad, host)
		}
	}
	return nil
}
