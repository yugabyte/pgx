package pgx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

const NO_SERVERS_MSG = "could not find a server to connect to"
const MAX_RETRIES = 20
const REFRESH_INTERVAL_SECONDS = 300

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
	// map of "region"."zone" -> slice of hostnames
	zoneList         map[string][]string
	unavailableHosts map[string]int
}

type lbHost struct {
	hostname string
	port     uint16
	err      error
}

var clustersLoadInfo map[string]*ClusterLoadInfo

const LB_QUERY = "SELECT * FROM yb_servers()"

var requestChan chan *ClusterLoadInfo
var hostChan chan *lbHost

func NewClusterLoadInfo(ctx context.Context, config *ConnConfig) *ClusterLoadInfo {
	info := new(ClusterLoadInfo)
	info.clusterName = config.Host
	info.ctx = ctx
	info.config = config
	return info
}

func init() {
	clustersLoadInfo = make(map[string]*ClusterLoadInfo)
	requestChan = make(chan *ClusterLoadInfo)
	hostChan = make(chan *lbHost)
	go produceHostName(requestChan, hostChan)
}

func produceHostName(in chan *ClusterLoadInfo, out chan *lbHost) {

	for {
		new, present := <-in

		if !present {
			log.Println("requestChannel closed")
			break
		}
		if new.ctx == nil {
			// This means the count needs to be decremented for the host.
			names := strings.Split(new.clusterName, ",")
			if len(names) != 2 {
				log.Printf("cannot parse names to update connection count: %s", new.clusterName)
			} else {
				cli, ok := clustersLoadInfo[names[0]]
				if ok {
					cnt := cli.hostLoad[names[1]]
					if cnt == 0 {
						log.Printf("connection count for %s going negative!", names[1])
					}
					clustersLoadInfo[names[0]].hostLoad[names[1]] = cnt - 1
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
			clustersLoadInfo[new.config.Host] = new

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
		return nil, lbHost.err
	}
	if lbHost.hostname == config.Host {
		conn, err := connect(ctx, config)
		if err != nil {
			decrementConnCount(config.controlHost + "," + config.Host)
		}
		return conn, err
	} else {
		var newConnString string
		if strings.Contains(config.connString, "@") {
			pattern := regexp.MustCompile("@([^/]*)/")
			newConnString = pattern.ReplaceAllString(config.connString, fmt.Sprintf("@%s:%d/", lbHost.hostname, lbHost.port))
		} else {
			pattern := regexp.MustCompile("://([^/]*)/")
			newConnString = pattern.ReplaceAllString(config.connString, fmt.Sprintf("://%s:%d/", lbHost.hostname, lbHost.port))
		}
		newConfig, err := ParseConfig(newConnString)
		if err != nil {
			return nil, err
		}
		newConfig.Port = lbHost.port
		newConfig.controlHost = config.controlHost
		conn, err := connect(ctx, newConfig)
		for i := 0; i < MAX_RETRIES && err != nil; i++ {
			decrementConnCount(newConfig.controlHost + "," + newConfig.Host)
			if conn != nil && conn.shouldLog(LogLevelWarn) {
				conn.log(ctx, LogLevelWarn, err.Error()+", retrying ...", nil)
			}
			newLoadInfo.unavailableHosts = map[string]int{lbHost.hostname: 1}
			requestChan <- newLoadInfo
			lbHost = <-hostChan
			if lbHost.err != nil {
				return nil, lbHost.err
			}
			if lbHost.hostname == newConfig.Host { // will always fail if multiple hostnames specified
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
		ctx:         nil,
	}
}

func refreshLoadInfo(li *ClusterLoadInfo) error {
	if li.controlConn == nil || li.controlConn.IsClosed() {
		var err error
		li.controlConn, err = connect(li.ctx, li.config)
		if err != nil {
			log.Printf("Could not connect to %s", li.config.Host)
			// remove its hostLoad entry
			cli, ok := clustersLoadInfo[li.config.Host]
			if ok {
				delete(cli.hostLoad, li.config.Host)
			}
			return err
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
	newMap := make(map[string]int)
	li.hostPort = make(map[string]uint16)
	li.zoneList = make(map[string][]string)
	for rows.Next() {
		err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
		if err != nil {
			log.Printf("Could not read load information: %s", err.Error())
			return err
		} else {
			if publicIP == "" {
				publicIP = host
			}
			tk := cloud + "." + region + "." + zone
			hosts, ok := li.zoneList[tk]
			if !ok {
				hosts = make([]string, 0)
			}
			hosts = append(hosts, publicIP)
			li.zoneList[tk] = hosts
			cnt := li.hostLoad[publicIP]
			newMap[publicIP] = cnt
			li.hostPort[publicIP] = uint16(port)
		}
	}
	li.hostLoad = newMap
	li.lastRefresh = time.Now()
	li.unavailableHosts = make(map[string]int) // clear the away hosts list
	return nil
}

func getHostWithLeastConns(li *ClusterLoadInfo) *lbHost {
	leastCnt := -1
	leastLoaded := ""
	if li.config.topologyKeys != nil {
		for _, tk := range li.config.topologyKeys {
			for _, h := range li.zoneList[tk] {
				if !isHostAway(li, h) && (leastCnt == -1 || li.hostLoad[h] < leastCnt) {
					leastLoaded, leastCnt = h, li.hostLoad[h]
				}
			}
		}
	} else {
		for h := range li.hostLoad {
			if !isHostAway(li, h) && (leastCnt == -1 || li.hostLoad[h] < leastCnt) {
				leastLoaded, leastCnt = h, li.hostLoad[h]
			}
		}
	}
	if leastLoaded == "" {
		lbh := &lbHost{
			hostname: leastLoaded,
			err:      errors.New(NO_SERVERS_MSG),
		}
		return lbh
	}
	lbh := &lbHost{
		hostname: leastLoaded,
		port:     li.hostPort[leastLoaded],
		err:      nil,
	}
	li.hostLoad[leastLoaded] = leastCnt + 1
	return lbh
}

func isHostAway(li *ClusterLoadInfo, h string) bool {
	for awayHost := range li.unavailableHosts {
		if h == awayHost {
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

// For test purpose
func ClearLoadBalanceInfo() {
	for k := range clustersLoadInfo {
		delete(clustersLoadInfo, k)
	}
}
