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

			for k := range new.zoneList {
				fmt.Printf("zonelist[" + k + "]: ")
				for _, e := range new.zoneList[k] {
					fmt.Print(e + ", ")
				}
			}
			out <- getHostWithLeastConns(new)
			// continue
		} else {
			old.config.topologyKeys = new.config.topologyKeys // Forget earlier topology-key specified.
			out <- getLeastLoadedHost(old)
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
		return connect(ctx, config)
	} else {
		nc := config.Copy() // todo Can we avoid this copy? Is it needed since this config is likely to be unused.
		nc.Host = lbHost.hostname
		nc.Port = lbHost.port
		return connect(ctx, nc)
	}
}

func refreshLoadInfo(li *ClusterLoadInfo) error {
	if li.controlConn == nil || li.controlConn.IsClosed() {
		var err error
		li.controlConn, err = connect(li.ctx, li.config)
		// defer li.controlConn.Close()
		if err != nil {
			log.Fatalf("Could not connect to %s", li.config.Host)
			// remove its hostLoad entry
			cli, ok := clustersLoadInfo[li.config.Host]
			if ok {
				delete(cli.hostLoad, li.config.Host)
			}
			return err
		}
	}

	rows, err := li.controlConn.Query(li.ctx, LB_QUERY)
	if err != nil {
		log.Fatalf("Could not query load information: %s", err.Error())
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
			log.Fatalf("Could not read load information: %s", err.Error())
			return err
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
	li.lastRefresh = time.Now()
	return nil
}

func getHostWithLeastConns(li *ClusterLoadInfo) *lbHost {
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
	if leastLoaded == "" {
		lbh := &lbHost{
			hostname: leastLoaded,
			err:      errors.New("could not find a server to connect to"),
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

func getLeastLoadedHost(li *ClusterLoadInfo) *lbHost {
	if time.Now().Second()-li.lastRefresh.Second() > 300 { // todo read 300 from a const or a param
		err := refreshLoadInfo(li)
		if err != nil {
			return &lbHost{
				hostname: "",
				err:      err,
			}
		}
	}

	return getHostWithLeastConns(li)
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
