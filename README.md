
# YugabyteDB Go Driver
This is a Go Driver based on [jackc/pgx](https://github.com/jackc/pgx), with following additional feature:

## Connection load balancing

Users can use this feature in two configurations.

### Cluster-aware / Uniform connection load balancing

In the cluster-aware connection load balancing, connections are distributed across all the tservers in the cluster, irrespective of their placements.

To enable the cluster-aware connection load balancing, provide the parameter `load_balance` with value as either `true` or `any` in the connection url or the connection string (DSN style). [This section](#read-replica-cluster) explains the different values for `load_balance` parameter.

```
"postgres://username:password@localhost:5433/database_name?load_balance=true"
```

With this parameter specified in the url, the driver will fetch and maintain the list of tservers from the given endpoint (`localhost` in above example) available in the YugabyteDB cluster and distribute the connections equally across them.

This list is refreshed every 5 minutes, when a new connection request is received.

Application needs to use the same connection url to create every connection it needs, so that the distribution happens equally.

### Topology-aware connection load balancing

With topology-aware connnection load balancing, users can target tservers in specific zones by specifying these zones as `topology_keys` with values in the format `cloudname.regionname.zonename`. Multiple zones can be specified as comma separated values.

The connections will be distributed equally with the tservers in these zones.

Note that, you would still need to specify `load_balance` to one of the 5 allowed values to enable the topology-aware connection load balancing.

```
"postgres://username:password@localhost:5433/database_name?load_balance=true&topology_keys=cloud1.region1.zone1,cloud1.region1.zone2"
```
### Specifying fallback zones

For topology-aware load balancing, you can now specify fallback placements too. This is not applicable for cluster-aware load balancing.
Each placement value can be suffixed with a colon (`:`) followed by a preference value between 1 and 10.
A preference value of `:1` means it is a primary placement. A preference value of `:2` means it is the first fallback placement and so on.If no preference value is provided, it is considered to be a primary placement (equivalent to one with preference value `:1`). Example given below.

```
"postgres://username:password@localhost:5433/database_name?load_balance=any&topology_keys=cloud1.region1.zone1:1,cloud1.region1.zone2:2";
```

You can also use `*` for specifying all the zones in a given region as shown below. This is not allowed for cloud or region values.

```
"postgres://username:password@localhost:5433/database_name?load_balance=any&topology_keys=cloud1.region1.*:1,cloud1.region2.*:2";
```

The driver attempts to connect to a node in following order: the least loaded node in the 1) primary placement(s), else in the 2) first fallback if specified, else in the 3) second fallback if specified and so on.
If no nodes are available either in primary placement(s) or in any of the fallback placements, then nodes in the rest of the cluster are attempted.
And this repeats for each connection request.

## Specifying Refresh Interval

Users can specify Refresh Time Interval, in seconds. It is the time interval between two attempts to refresh the information about cluster nodes. Default is 300. Valid values are integers between 0 and 600. Value 0 means refresh for each connection request. Any value outside this range is ignored and the default is used.

To specify Refresh Interval, use the parameter `yb_servers_refresh_interval` in the connection url or the connection string.

```
"postgres://username:password@localhost:5433/database_name?yb_servers_refresh_interval=X&load_balance=true&topology_keys=cloud1.region1.*:1,cloud1.region2.*:2";
```

Same parameters can be specified in the connection url while using the `pgxpool.Connect()` API.

## Other Connection Parameters:

### fallback_to_topology_keys_only
Applicable only for TopologyAware Load Balancing. When set to true, the smart driver does not attempt to connect to servers outside of primary and fallback placements specified via property. The default behaviour is to fallback to any available server in the entire cluster.(default value: false)

### failed_host_reconnect_delay_secs
The driver marks a server as failed with a timestamp, when it cannot connect to it. Later, whenever it refreshes the server list via yb_servers(), if it sees the failed server in the response, it marks the server as UP only if failed-host-reconnect-delay-secs time has elapsed. (The yb_servers() function does not remove a failed server immediately from its result and retains it for a while.)(default value: 5 seconds)

## Read Replica Cluster

PGX smart driver also enables load balancing across nodes in primary clusters which have associated Read Replica cluster.

The connection property `load-balance` allows five values using which users can distribute connections among different combination of nodes as per their requirements:

- `only-rr` - Create connections only on Read Replica nodes
- `only-primary` - Create connections only on primary cluster nodes
- `prefer-rr` - Create connections on Read Replica nodes. If none available, on any node in the cluster including primary cluster nodes
- `prefer-primary` - Create connections on primary cluster nodes. If none available, on any node in the cluster including Read Replica nodes
- `any` or `true` - Equivalent to value true. Create connections on any node in the primary or Read Replica cluster

default value is false

Details about the upstream pgx driver - which hold true for this driver as well - are given below.

# pgx - PostgreSQL Driver and Toolkit

pgx is a pure Go driver and toolkit for PostgreSQL.

The pgx driver is a low-level, high performance interface. It also includes an adapter for the standard `database/sql` interface.

The toolkit component is a related set of packages that implement PostgreSQL functionality such as parsing the wire protocol
and type mapping between PostgreSQL and Go. These underlying packages can be used to implement alternative drivers,
proxies, load balancers, logical replication clients, etc.

## Example Usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yugabyte/pgx/v5"
)

func main() {
	// urlExample := "postgres://username:password@localhost:5433/database_name"
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(name, weight)
}
```

See the [getting started guide](https://github.com/jackc/pgx/wiki/Getting-started-with-pgx) for more information.

## Features

* Support for approximately 70 different PostgreSQL types
* Automatic statement preparation and caching
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (allows for much quicker encoding/decoding)
* `COPY` protocol support for faster bulk data loads
* Tracing and logging support
* Connection pool with after-connect hook for arbitrary connection setup
* Conversion of PostgreSQL arrays to Go slice mappings for integers, floats, and strings
* `hstore` support
* `json` and `jsonb` support
* Maps `inet` and `cidr` PostgreSQL types to `netip.Addr` and `netip.Prefix`
* Large object support
* NULL mapping to pointer to pointer
* Supports `database/sql.Scanner` and `database/sql/driver.Valuer` interfaces for custom types
* Notice response handling
* Simulated nested transactions with savepoints

## Choosing Between the pgx and database/sql Interfaces

The pgx interface is faster.

The pgx interface is recommended when:

1. The application only targets PostgreSQL.
2. No other libraries that require `database/sql` are in use.

It is also possible to use the `database/sql` interface and convert a connection to the lower-level pgx interface as needed.

## Testing

See CONTRIBUTING.md for setup instructions.

## Architecture

See the presentation at Golang Estonia, [PGX Top to Bottom](https://www.youtube.com/watch?v=sXMSWhcHCf8) for a description of pgx architecture.

## Supported Go and PostgreSQL Versions

pgx supports the same versions of Go and PostgreSQL that are supported by their respective teams. For [Go](https://golang.org/doc/devel/release.html#policy) that is the two most recent major releases and for [PostgreSQL](https://www.postgresql.org/support/versioning/) the major releases in the last 5 years. This means pgx supports Go 1.20 and higher and PostgreSQL 12 and higher.

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. `v5` is the latest stable major version.

## PGX Family Libraries

### [github.com/jackc/pglogrepl](https://github.com/jackc/pglogrepl)

pglogrepl provides functionality to act as a client for PostgreSQL logical replication.

pgx supports the same versions of Go and PostgreSQL that are supported by their respective teams. For [Go](https://golang.org/doc/devel/release.html#policy) that is the two most recent major releases and for [PostgreSQL](https://www.postgresql.org/support/versioning/) the major releases in the last 5 years. This means pgx supports Go 1.16 and higher and PostgreSQL 10 and higher.

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

### [github.com/jackc/tern](https://github.com/jackc/tern)

tern is a stand-alone SQL migration system.

### [github.com/jackc/pgerrcode](https://github.com/jackc/pgerrcode)

pgerrcode contains constants for the PostgreSQL error codes.

## Adapters for 3rd Party Types

### [github.com/yugabyte/pgx/v4/pgxpool](https://github.com/yugabyte/pgx/tree/master/pgxpool)


### [github.com/yugabyte/pgx/v4/stdlib](https://github.com/yugabyte/pgx/tree/master/stdlib)

* [https://github.com/jackhopner/pgx-xray-tracer](https://github.com/jackhopner/pgx-xray-tracer)

## Adapters for 3rd Party Loggers

These adapters can be used with the tracelog package.

* [github.com/jackc/pgx-go-kit-log](https://github.com/jackc/pgx-go-kit-log)
* [github.com/jackc/pgx-log15](https://github.com/jackc/pgx-log15)
* [github.com/jackc/pgx-logrus](https://github.com/jackc/pgx-logrus)
* [github.com/jackc/pgx-zap](https://github.com/jackc/pgx-zap)
* [github.com/jackc/pgx-zerolog](https://github.com/jackc/pgx-zerolog)
* [github.com/mcosta74/pgx-slog](https://github.com/mcosta74/pgx-slog)
* [github.com/kataras/pgx-golog](https://github.com/kataras/pgx-golog)

## 3rd Party Libraries with PGX Support

### [github.com/pashagolub/pgxmock](https://github.com/pashagolub/pgxmock)

pgxmock is a mock library implementing pgx interfaces.
pgxmock has one and only purpose - to simulate pgx behavior in tests, without needing a real database connection.

### [github.com/georgysavva/scany](https://github.com/georgysavva/scany)

Library for scanning data from a database into Go structs and more.

### [github.com/vingarcia/ksql](https://github.com/vingarcia/ksql)

A carefully designed SQL client for making using SQL easier,
more productive, and less error-prone on Golang.

### [https://github.com/otan/gopgkrb5](https://github.com/otan/gopgkrb5)

Adds GSSAPI / Kerberos authentication support.

### [github.com/wcamarao/pmx](https://github.com/wcamarao/pmx)

Explicit data mapping and scanning library for Go structs and slices.

### [github.com/stephenafamo/scan](https://github.com/stephenafamo/scan)

Type safe and flexible package for scanning database data into Go types.
Supports, structs, maps, slices and custom mapping functions.

### [https://github.com/z0ne-dev/mgx](https://github.com/z0ne-dev/mgx)

Code first migration library for native pgx (no database/sql abstraction).
