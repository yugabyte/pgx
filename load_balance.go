package pgx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"regexp"
	"strings"
	"time"
)

const NO_SERVERS_MSG = "could not find a server to connect to"
const MAX_RETRIES = 20
const REFRESH_INTERVAL_SECONDS = 300

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
	lastRefresh time.Time
	// map of host -> connection count
	hostLoad map[string]int
	// map of host -> port
	hostPort map[string]uint16
	// map of "cloud.region.zone" -> slice of hostnames
	zoneList map[string][]string
	// map of host -> int
	unavailableHosts map[string]int
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

const LB_QUERY = "SELECT * FROM yb_servers()"

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
		return connect(ctx, config) // fallback to original behaviour
	}
	if lbHost.hostname == config.Host {
		conn, err := connect(ctx, config)
		if err != nil {
			decrementConnCount(config.controlHost + "," + config.Host)
		}
		return conn, err
	} else {
		newConnString := replaceHostString(config.connString, lbHost.hostname, lbHost.port)
		newConfig, err := ParseConfig(newConnString)
		if err != nil {
			return nil, err
		}
		newConfig.Port = lbHost.port
		newConfig.controlHost = config.controlHost
		conn, err := connect(ctx, newConfig)
		for i := 0; i < MAX_RETRIES && err != nil; i++ {
			decrementConnCount(newConfig.controlHost + "," + newConfig.Host)
			// log.Println(err.Error() + ", retrying ...")
			newLoadInfo.unavailableHosts = map[string]int{lbHost.hostname: 1}
			requestChan <- newLoadInfo
			lbHost = <-hostChan
			if lbHost.err != nil {
				return nil, lbHost.err
			}
			if lbHost.hostname == newConfig.Host {
				conn, err := connect(ctx, newConfig)
				if err != nil {
					decrementConnCount(newConfig.controlHost + "," + newConfig.Host)
				}
				return conn, err
			}
			newConnString = strings.Replace(newConfig.connString, newConfig.Host, lbHost.hostname, -1)
			newConfig, err = ParseConfig(newConnString)
			if err != nil {
				return nil, err
			}
			newConfig.Port = lbHost.port
			newConfig.controlHost = config.controlHost
			conn, err = connect(ctx, newConfig)
		}
		if err != nil {
			decrementConnCount(newConfig.controlHost + "," + newConfig.Host)
		}
		return conn, err
	}
}

func decrementConnCount(str string) {
	requestChan <- &ClusterLoadInfo{
		clusterName: str,
		flags:       DECREMENT_COUNT,
	}
}

func refreshLoadInfo(li *ClusterLoadInfo) error {
	if li.controlConn == nil || li.controlConn.IsClosed() {
		var err error
		li.controlConn, err = connect(li.ctx, li.config)
		if err != nil {
			log.Printf("Could not create control connect to %s\n", li.config.Host)
			// remove its hostLoad entry
			cli, ok := clustersLoadInfo[li.clusterName]
			if ok {
				delete(cli.hostLoad, li.config.Host)
			}
			// Attempt connection to other servers which are already fetched in cli.
			if len(li.hostPairs) > 0 {
				log.Println("Attempting control connection to other servers ...")
			}
			var config *ConnConfig
			for h := range li.hostPairs {
				newConnString := replaceHostString(li.config.connString, h, li.hostPort[h])
				if config, err = ParseConfig(newConnString); err == nil {
					if li.controlConn, err = connect(li.ctx, config); err == nil {
						break
					}
					delete(li.hostLoad, config.Host)
					li.controlConn = nil
				}
			}
			if err != nil {
				return err
			}
		}
	}
	// defer li.controlConn.Close(li.ctx)

	rows, err := li.controlConn.Query(li.ctx, LB_QUERY)
	if err != nil {
		log.Printf("Could not query load information: %s", err.Error())
		return err
	}
	defer rows.Close()
	var host, nodeType, cloud, region, zone, publicIP string
	var port, numConns int
	newHostLoad := make(map[string]int)
	li.hostPort = make(map[string]uint16)
	li.zoneList = make(map[string][]string)
	li.hostPairs = make(map[string]string)
	for rows.Next() {
		err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
		if err != nil {
			log.Printf("Could not read load information: %s", err.Error())
			return err
		} else {
			li.hostPairs[host] = publicIP
			tk := cloud + "." + region + "." + zone
			hosts, ok := li.zoneList[tk]
			if !ok {
				hosts = make([]string, 0)
			}
			hosts = append(hosts, host)
			li.zoneList[tk] = hosts
			cnt := li.hostLoad[host]
			newHostLoad[host] = cnt
			li.hostPort[host] = uint16(port)
		}
	}
	li.hostLoad = newHostLoad
	li.lastRefresh = time.Now()
	li.unavailableHosts = make(map[string]int) // clear the unavailable-hosts list
	return nil
}

func getHostWithLeastConns(li *ClusterLoadInfo) *lbHost {
	leastCnt := int(math.MaxInt64)
	leastLoaded := ""
	if li.config.topologyKeys != nil {
		for _, tk := range li.config.topologyKeys {
			for _, h := range li.zoneList[tk] {
				if !isHostAway(li, h) && li.hostLoad[h] < leastCnt {
					leastLoaded, leastCnt = h, li.hostLoad[h]
				}
			}
		}
	} else {
		for h := range li.hostLoad {
			if !isHostAway(li, h) && li.hostLoad[h] < leastCnt {
				leastLoaded, leastCnt = h, li.hostLoad[h]
			}
		}
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

func refreshAndGetLeastLoadedHost(li *ClusterLoadInfo, awayHosts map[string]int) *lbHost {
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
		li.unavailableHosts[h] = 1
	}
	return getHostWithLeastConns(li)
}

// expects the toplogykeys in the format 'cloud1.region1.zone1,cloud1.region1.zone2,...'
func validateTopologyKeys(s string) ([]string, error) {
	tkeys := strings.Split(s, ",")
	for _, tk := range tkeys {
		zones := strings.Split(tk, ".")
		if len(zones) != 3 {
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
			newzl := make([]string, len(hosts))
			copy(newzl, hosts)
			az[n][z] = newzl
		}
	}
	return az
}
