package pgx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

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
	zoneList map[string][]string
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

func New(ctx context.Context, config *ConnConfig) *ClusterLoadInfo {
	info := new(ClusterLoadInfo)
	info.clusterName = config.Host
	info.ctx = ctx
	info.config = config
	info.hostLoad = make(map[string]int)
	info.hostPort = make(map[string]uint16)
	info.zoneList = make(map[string][]string)
	return info
}

func produceHostName(in chan *ClusterLoadInfo, out chan *lbHost) {

	for {
		log.Println("Waiting to read LoadInfo from channel ...")
		new, ok := <-in

		if !ok {
			log.Println("requestChannel closed")
			break
		}
		if new.ctx == nil {
			// This means the count needs to be decremented for the host.
			names := strings.Split(new.clusterName, ",")
			if len(names) != 2 {
				log.Fatalf("cannot parse names to update connection count: %s", new.clusterName)
			} else {
				cnt := clustersLoadInfo[names[0]].hostLoad[names[1]]
				log.Printf("Decrementing count (%d) for %s by 1", cnt, names[1])
				clustersLoadInfo[names[0]].hostLoad[names[1]] = cnt - 1
			}
			continue
		}
		old, ok := clustersLoadInfo[new.config.Host]
		if !ok {
			// There is no loadInfo available for this config. Create one.
			log.Println("Load Info not available in map, initializing it...")
			li := new
			ctx := li.ctx
			config := li.config
			var err error
			clustersLoadInfo[config.Host] = li
			// li = commonLoadInfo[config.Host]

			li.controlConn, err = connect(ctx, config)
			if err != nil {
				log.Fatalf("Could not connect to %s", config.Host)
				delete(clustersLoadInfo, config.Host)
				lb := &lbHost{
					hostname: "",
					err:      err,
				}
				out <- lb
				continue
			}

			rows, err := li.controlConn.Query(ctx, LB_QUERY)
			if err != nil {
				log.Fatalf("Could not query load information: %s", err.Error())
				lb := &lbHost{
					hostname: "",
					err:      err,
				}
				out <- lb
				continue
			}
			defer rows.Close()

			var host, nodeType, cloud, region, zone, publicIP, leastLoaded string
			var port, numConns uint16
			for rows.Next() {
				err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
				if err != nil {
					log.Fatalf("Could not read load information %s", err.Error())
					lb := &lbHost{
						hostname: "",
						err:      err,
					}
					out <- lb
					break
				} else {
					if publicIP == "" {
						publicIP = host
					}
					hosts, ok := li.zoneList[region+"."+zone]
					if !ok {
						log.Println("Zonelist for " + region + "." + zone + " not found, initializing it...")
						hosts = make([]string, 0)
					}
					hosts = append(hosts, publicIP)
					li.zoneList[region+"."+zone] = hosts

					li.hostLoad[publicIP] = 0
					li.hostPort[publicIP] = port
					// pick the first host as the least loaded since this would be the first connection anyway.
					if leastLoaded == "" {
						if li.config.topologyKeys == "" {
							li.hostLoad[publicIP] = 1
							leastLoaded = publicIP
						} else if li.config.topologyKeys == fmt.Sprintf("%s.%s", region, zone) {
							li.hostLoad[publicIP] = 1
							leastLoaded = publicIP
						}
					}
				}
			}
			li.lastRefresh = time.Now()
			for k := range li.zoneList {
				fmt.Printf("\nzonelist-" + k + ": ")
				for _, e := range li.zoneList[k] {
					fmt.Print(e + ", ")
				}
			}
			if leastLoaded == "" {
				lb := &lbHost{
					hostname: leastLoaded,
					err:      errors.New("could not find a server to connect to"),
				}
				out <- lb
			} else {
				newConfig := config.Copy()
				newConfig.Host = leastLoaded
				lb := &lbHost{
					hostname: leastLoaded,
					port:     li.hostPort[leastLoaded],
					err:      nil,
				}
				out <- lb
			}
			// continue
		} else {
			host, err := getLeastLoadedHost(old)
			lb := &lbHost{
				hostname: host,
				port:     old.hostPort[host],
				err:      err,
			}
			out <- lb
		}
	}
}

func init() {
	clustersLoadInfo = make(map[string]*ClusterLoadInfo)
	requestChan = make(chan *ClusterLoadInfo)
	hostChan = make(chan *lbHost)
	go produceHostName(requestChan, hostChan)
}

func connectLoadBalanced(ctx context.Context, config *ConnConfig) (c *Conn, err error) {
	newLoadInfo := New(ctx, config)
	requestChan <- newLoadInfo
	lbHost := <-hostChan
	if lbHost.err != nil {
		return nil, lbHost.err
	}
	if lbHost.hostname == config.Host {
		return connect(ctx, config)
	} else {
		nc := config.Copy()
		nc.Host = lbHost.hostname
		nc.Port = lbHost.port
		return connect(ctx, nc)
	}
}

func getLeastLoadedHost(li *ClusterLoadInfo) (string, error) {
	if time.Now().Second()-li.lastRefresh.Second() > 300 {
		if li.controlConn.IsClosed() {
			var err error
			li.controlConn, err = connect(li.ctx, li.config)
			if err != nil {
				log.Fatalf("Could not connect to %s", li.config.Host)
				return "", err
			}
		}

		rows, err := li.controlConn.Query(li.ctx, LB_QUERY)
		if err != nil {
			log.Fatalf("Could not query load information: %s", err.Error())
			return "", err
		}
		defer rows.Close()
		var host, nodeType, cloud, region, zone, publicIP string
		var port, numConns int
		newMap := make(map[string]int)
		li.hostPort = make(map[string]uint16)
		li.zoneList = make(map[string][]string) // discard old zonelist. Can be optimized?
		for rows.Next() {
			err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
			if err != nil {
				log.Fatalf("Could not read load information: %s", err.Error())
				return "", err
			} else {
				if publicIP == "" {
					publicIP = host
				}
				hosts, ok := li.zoneList[region+"."+zone]
				if !ok {
					hosts = make([]string, 0)
				}
				hosts = append(hosts, publicIP)
				li.zoneList[region+"."+zone] = hosts
				cnt := li.hostLoad[publicIP]
				log.Printf("Updating host info: [%s] = %d", publicIP, cnt)
				newMap[publicIP] = cnt
				li.hostPort[publicIP] = uint16(port)
			}
		}
		li.hostLoad = newMap
	}

	leastCnt := -1
	leastLoaded := ""
	if li.config.topologyKeys != "" {
		for _, h := range li.zoneList[li.config.topologyKeys] {
			if leastCnt == -1 || li.hostLoad[h] < leastCnt {
				leastCnt = li.hostLoad[h]
				leastLoaded = h
			}
		}
	} else {
		for h := range li.hostLoad {
			if leastCnt == -1 || li.hostLoad[h] < leastCnt {
				leastCnt = li.hostLoad[h]
				leastLoaded = h
			}
		}
	}
	li.hostLoad[leastLoaded] = leastCnt + 1
	return leastLoaded, nil
}

func validateTopologyKeys(s string) error {
	zones := strings.Split(s, ".")
	if len(zones) != 2 {
		return errors.New("toplogy_keys '" + s + "' not in correct format, should be specified as <regionname>.<zonename>")
	}
	return nil
}

func PrintHostLoad() {
	for k := range clustersLoadInfo {
		str := "For cluster " + k + ": "
		for h := range clustersLoadInfo[k].hostLoad {
			str = str + h + "=" + strconv.Itoa(clustersLoadInfo[k].hostLoad[h]) + ", "
		}
		log.Println(str)
	}
}

func ClearLoadBalanceInfo() {
	for k := range clustersLoadInfo {
		delete(clustersLoadInfo, k)
	}
}
