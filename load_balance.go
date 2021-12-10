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

type LoadInfo struct {
	clusterName string
	ctx         context.Context
	config      *ConnConfig
	controlConn *Conn
	lastrefresh time.Time
	hostLoad    map[string]int
	zoneList    map[string][]string
}

type lbHost struct {
	hostname string
	err      error
}

var commonLoadInfo map[string]*LoadInfo

const LB_QUERY = "SELECT * FROM yb_servers()"

var requestChan chan *LoadInfo
var hostChan chan *lbHost

func New(ctx context.Context, config *ConnConfig) *LoadInfo {
	info := new(LoadInfo)
	info.clusterName = config.Host
	info.ctx = ctx
	info.config = config
	info.hostLoad = make(map[string]int)
	info.zoneList = make(map[string][]string)
	log.Println("returning New LoadInfo ...")
	return info
}

func produceHostName(in chan *LoadInfo, out chan *lbHost) {

	for {
		log.Println("Waiting to read LoadInfo from channel ...")
		new, ok := <-in

		if !ok {
			log.Println("requestChannel closed")
			break
		}
		if new.ctx == nil {
			// This means the count needs to be decremented for the host.
			// We don't know the cluster, so iterate through all the clusters
			done := false
			for k := range commonLoadInfo {
				for h := range commonLoadInfo[k].hostLoad {
					if h == new.clusterName { // clusterName is the hostname whose connection is closed
						cnt := commonLoadInfo[k].hostLoad[h]
						log.Printf("Decrementing count (%d) for %s by 1", cnt, h)
						commonLoadInfo[k].hostLoad[h] = cnt - 1
						done = true
						break
					}
				}
				if done {
					break
				}
			}
			continue
		}
		old, ok := commonLoadInfo[new.config.Host]
		if !ok {
			// There is no loadInfo available for this config. Create one.
			log.Println("Load Info not available in map, initializing it...")
			li := new
			ctx := li.ctx
			config := li.config
			var err error
			commonLoadInfo[config.Host] = li
			// li = commonLoadInfo[config.Host]

			li.controlConn, err = connect(ctx, config)
			if err != nil {
				log.Fatalf("Could not connect to %s", config.Host)
				delete(commonLoadInfo, config.Host)
				lb := &lbHost{"", err}
				out <- lb
				continue
			}

			rows, err := li.controlConn.Query(ctx, LB_QUERY)
			if err != nil {
				log.Fatalf("Could not query load information: %s", err.Error())
				lb := &lbHost{"", err}
				out <- lb
				continue
			}
			defer rows.Close()

			var host, nodeType, cloud, region, zone, publicIP, leastLoaded string
			var port, numConns int
			for rows.Next() {
				err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
				if err != nil {
					log.Fatalf("Could not read load information %s", err.Error())
					lb := &lbHost{"", err}
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
					// pick the first host as the least loaded since this is the first connection anyway.
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
			li.lastrefresh = time.Now()
			for k := range li.zoneList {
				fmt.Printf("\nzonelist-" + k + ": ")
				for _, e := range li.zoneList[k] {
					fmt.Print(e + ", ")
				}
			}
			if leastLoaded == "" {
				lb := &lbHost{leastLoaded, errors.New("could not find server to connect to.")}
				out <- lb
			} else {
				newConfig := config.Copy()
				newConfig.Host = leastLoaded
				lb := &lbHost{leastLoaded, nil}
				out <- lb
			}
			// continue
		} else {
			host, err := getLeastLoadedHost(old)
			lb := &lbHost{host, err}
			out <- lb
		}
	}
}

func init() {
	commonLoadInfo = make(map[string]*LoadInfo)
	requestChan = make(chan *LoadInfo)
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
		return connect(ctx, nc)
	}
}

func getLeastLoadedHost(li *LoadInfo) (string, error) {
	if time.Now().Second()-li.lastrefresh.Second() > 300 {
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
	for k := range commonLoadInfo {
		str := "For cluster " + k + ": "
		for h := range commonLoadInfo[k].hostLoad {
			str = str + h + "=" + strconv.Itoa(commonLoadInfo[k].hostLoad[h]) + ", "
		}
		log.Println(str)
	}
}

func ClearLoadBalanceInfo() {
	for k := range commonLoadInfo {
		delete(commonLoadInfo, k)
	}
}
