package pgx

import (
	"context"
	"log"
	"strconv"
	"time"
)

type LoadInfo struct {
	clusterName string
	ctx         context.Context
	config      *ConnConfig
	controlConn *Conn
	lastrefresh time.Time
	hostLoad    map[string]int
}

type lbHost struct {
	hostname string
	err      error
}

var commonLoadInfo map[string]*LoadInfo

const LB_QUERY = "SELECT * FROM yb_servers()"

var requestChan chan *LoadInfo
var hostChan chan *lbHost

// var loadInfo *LoadInfo
// var controlConn *Conn

func New(ctx context.Context, config *ConnConfig) *LoadInfo {
	info := new(LoadInfo)
	info.clusterName = config.Host
	info.ctx = ctx
	info.config = config
	info.hostLoad = make(map[string]int)
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
		old, okk := commonLoadInfo[new.config.Host]
		if !okk {
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
					log.Printf("Updating host info: [%s] = 0", publicIP)
					li.hostLoad[publicIP] = 0
					if leastLoaded == "" { // pick the first host as the least loaded since this is the first connection anyway.
						li.hostLoad[publicIP] = 1
						leastLoaded = publicIP
					}
				}
			}
			li.lastrefresh = time.Now()
			log.Printf("Found the least loaded server: %s (size of map %d)", leastLoaded, len(li.hostLoad))
			newConfig := config.Copy()
			newConfig.Host = leastLoaded
			lb := &lbHost{leastLoaded, nil}
			out <- lb
			// continue
		} else {
			log.Printf("Load Info available, getting a load-balanaced connection (size of map %d) ...", len(old.hostLoad))
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
	// loadInfo = New("default")
	go produceHostName(requestChan, hostChan)
}

func connectLoadBalanced(ctx context.Context, config *ConnConfig) (c *Conn, err error) {

	log.Printf("config.Host %s", config.Host)
	newLI := New(ctx, config)
	requestChan <- newLI
	log.Println("Written LoadInfo to channel, waiting to read lbHost now ...")
	lbHost := <-hostChan
	log.Printf("Received lbHost from map. Size of map %d", len(commonLoadInfo[config.Host].hostLoad))
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

	/* ----------------------

	// todo take lock
	log.Printf("config.Host %s", config.Host)
	li, ok := commonLoadInfo[config.Host]
	if !ok { // there is no loadInfo available for this config. Create one.
		log.Println("Load Info not available, initializing it...")
		commonLoadInfo[config.Host] = New(ctx, config)
		li := commonLoadInfo[config.Host]

		li.controlConn, err = connect(ctx, config)
		if err != nil {
			log.Fatalf("Could not connect to %s", config.Host)
			delete(commonLoadInfo, config.Host)
			return nil, err
		}

		rows, err := li.controlConn.Query(ctx, LB_QUERY)
		if err != nil {
			log.Fatalf("Could not query load information: %s", err.Error())
			return nil, err
		}
		defer rows.Close()
		var host, nodeType, cloud, region, zone, publicIP, leastLoaded string
		var port, numConns int
		for rows.Next() {
			err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
			if err != nil {
				log.Fatalf("Could not read load information %s", err.Error())
				return nil, err
			} else {
				if publicIP == "" {
					publicIP = host
				}
				log.Printf("Updating host info: [%s] = 0", publicIP)
				li.hostLoad[publicIP] = 0
				if leastLoaded == "" { // pick the first host as the least loaded since this is the first connection anyway.
					li.hostLoad[publicIP] = 1
					leastLoaded = publicIP
				}
			}
		}
		li.lastrefresh = time.Now()
		log.Printf("Connecting to the least loaded server: %s ...", leastLoaded)
		newConfig := config.Copy()
		newConfig.Host = leastLoaded
		return connect(ctx, newConfig)
	}

	log.Println("Load Info available, getting a load-balanaced connection...")
	host, err := getLeastLoadedHost(li)
	newConfig := config.Copy()
	newConfig.Host = host
	return connect(ctx, newConfig)

	//  todo handle li.controlConn.IsClosed()

	// todo ping this connection in the background to retain the connection
	// todo release lock

	// return nil, nil
	---------------------- */
}

func getLeastLoadedHost(li *LoadInfo) (string, error) {
	if time.Now().Second()-li.lastrefresh.Second() > 300 {
		log.Println("Refresh of load info is needed")
		if li.controlConn.IsClosed() {
			log.Println("Control connection is closed, creating a new one...")
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
		var host, nodeType, cloud, region, zone, publicIP, leastLoaded string
		var port, numConns int
		leastCnt := -1
		newMap := make(map[string]int)
		for rows.Next() {
			err := rows.Scan(&host, &port, &numConns, &nodeType, &cloud, &region, &zone, &publicIP)
			if err != nil {
				log.Fatalf("Could not read load information: %s", err.Error())
				return "", err
			} else {
				if publicIP == "" {
					publicIP = host
				}
				cnt := li.hostLoad[publicIP]
				log.Printf("Updating host info: [%s] = %d", publicIP, cnt)
				newMap[publicIP] = cnt
				if leastCnt == -1 || cnt < leastCnt {
					leastCnt = cnt
					leastLoaded = publicIP
				}
			}
		}
		li.hostLoad = newMap
		return leastLoaded, nil
	}

	log.Println("Refresh of load info is not needed")
	leastCnt := -1
	leastLoaded := ""
	for k := range li.hostLoad {
		if leastCnt == -1 || li.hostLoad[k] < leastCnt {
			leastCnt = li.hostLoad[k]
			leastLoaded = k
		}
	}
	log.Printf("Least loaded host %s with connection count %d", leastLoaded, leastCnt)
	li.hostLoad[leastLoaded] = leastCnt + 1
	return leastLoaded, nil
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
	// take lock
	for k := range commonLoadInfo {
		delete(commonLoadInfo, k)
	}
	// release lock
}
