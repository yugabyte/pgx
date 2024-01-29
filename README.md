
# YugabyteDB Go Driver
This is a Go Driver based on [jackc/pgx](https://github.com/jackc/pgx), with following additional feature:

## Connection load balancing

Users can use this feature in two configurations.

### Cluster-aware / Uniform connection load balancing

In the cluster-aware connection load balancing, connections are distributed across all the tservers in the cluster, irrespective of their placements.

To enable the cluster-aware connection load balancing, provide the parameter `load_balance` set to true as `load_balance=true` in the connection url or the connection string (DSN style).

```
"postgres://username:password@localhost:5433/database_name?load_balance=true"
```

With this parameter specified in the url, the driver will fetch and maintain the list of tservers from the given endpoint (`localhost` in above example) available in the YugabyteDB cluster and distribute the connections equally across them.

This list is refreshed every 5 minutes, when a new connection request is received.

Application needs to use the same connection url to create every connection it needs, so that the distribution happens equally.

### Topology-aware connection load balancing

With topology-aware connnection load balancing, users can target tservers in specific zones by specifying these zones as `topology_keys` with values in the format `cloudname.regionname.zonename`. Multiple zones can be specified as comma separated values.

The connections will be distributed equally with the tservers in these zones.

Note that, you would still need to specify `load_balance=true` to enable the topology-aware connection load balancing.

```
"postgres://username:password@localhost:5433/database_name?load_balance=true&topology_keys=cloud1.region1.zone1,cloud1.region1.zone2"
```
### Specifying fallback zones

For topology-aware load balancing, you can now specify fallback placements too. This is not applicable for cluster-aware load balancing.
Each placement value can be suffixed with a colon (`:`) followed by a preference value between 1 and 10.
A preference value of `:1` means it is a primary placement. A preference value of `:2` means it is the first fallback placement and so on.If no preference value is provided, it is considered to be a primary placement (equivalent to one with preference value `:1`). Example given below.

```
"postgres://username:password@localhost:5433/database_name?load_balance=true&topology_keys=cloud1.region1.zone1:1,cloud1.region1.zone2:2";
```

You can also use `*` for specifying all the zones in a given region as shown below. This is not allowed for cloud or region values.

```
"postgres://username:password@localhost:5433/database_name?load_balance=true&topology_keys=cloud1.region1.*:1,cloud1.region2.*:2";
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

For a working example which demonstrates both the configurations of connection load balancing using `pgx.Connect()` and `pgxpool.Connect()`, see the [driver-examples](https://github.com/yugabyte/driver-examples/tree/main/go/pgx) repository.

Details about the upstream pgx driver - which hold true for this driver as well - are given below.

# pgx - PostgreSQL Driver and Toolkit

pgx is a pure Go driver and toolkit for PostgreSQL.

pgx aims to be low-level, fast, and performant, while also enabling PostgreSQL-specific features that the standard `database/sql` package does not allow for.

The driver component of pgx can be used alongside the standard `database/sql` package.

The toolkit component is a related set of packages that implement PostgreSQL functionality such as parsing the wire protocol
and type mapping between PostgreSQL and Go. These underlying packages can be used to implement alternative drivers,
proxies, load balancers, logical replication clients, etc.

The current release of `pgx v4` requires Go modules. To use the previous version, checkout and vendor the `v3` branch.

## Example Usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yugabyte/pgx/v4"
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

## Choosing Between the pgx and database/sql Interfaces

It is recommended to use the pgx interface if:
1. The application only targets PostgreSQL.
2. No other libraries that require `database/sql` are in use.

The pgx interface is faster and exposes more features.

The `database/sql` interface only allows the underlying driver to return or receive the following types: `int64`,
`float64`, `bool`, `[]byte`, `string`, `time.Time`, or `nil`. Handling other types requires implementing the
`database/sql.Scanner` and the `database/sql/driver/driver.Valuer` interfaces which require transmission of values in text format. The binary format can be substantially faster, which is what the pgx interface uses.

## Features

pgx supports many features beyond what is available through `database/sql`:

* Support for approximately 70 different PostgreSQL types
* Automatic statement preparation and caching
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (allows for much quicker encoding/decoding)
* COPY protocol support for faster bulk data loads
* Extendable logging support including built-in support for `log15adapter`, [`logrus`](https://github.com/sirupsen/logrus), [`zap`](https://github.com/uber-go/zap), and [`zerolog`](https://github.com/rs/zerolog)
* Connection pool with after-connect hook for arbitrary connection setup
* Listen / notify
* Conversion of PostgreSQL arrays to Go slice mappings for integers, floats, and strings
* Hstore support
* JSON and JSONB support
* Maps `inet` and `cidr` PostgreSQL types to `net.IPNet` and `net.IP`
* Large object support
* NULL mapping to Null* struct or pointer to pointer
* Supports `database/sql.Scanner` and `database/sql/driver.Valuer` interfaces for custom types
* Notice response handling
* Simulated nested transactions with savepoints

## Performance

There are three areas in particular where pgx can provide a significant performance advantage over the standard
`database/sql` interface and other drivers:

1. PostgreSQL specific types - Types such as arrays can be parsed much quicker because pgx uses the binary format.
2. Automatic statement preparation and caching - pgx will prepare and cache statements by default. This can provide an
   significant free improvement to code that does not explicitly use prepared statements. Under certain workloads, it can
   perform nearly 3x the number of queries per second.
3. Batched queries - Multiple queries can be batched together to minimize network round trips.

## Testing

pgx tests naturally require a PostgreSQL database. It will connect to the database specified in the `PGX_TEST_DATABASE` environment
variable. The `PGX_TEST_DATABASE` environment variable can either be a URL or DSN. In addition, the standard `PG*` environment
variables will be respected. Consider using [direnv](https://github.com/direnv/direnv) to simplify environment variable
handling.

### Example Test Environment

Connect to your PostgreSQL server and run:

```
create database pgx_test;
```

Connect to the newly-created database and run:

```
create domain uint64 as numeric(20,0);
```

Now, you can run the tests:

```
PGX_TEST_DATABASE="host=/var/run/postgresql database=pgx_test" go test ./...
```

In addition, there are tests specific for PgBouncer that will be executed if `PGX_TEST_PGBOUNCER_CONN_STRING` is set.

## Supported Go and PostgreSQL Versions

pgx supports the same versions of Go and PostgreSQL that are supported by their respective teams. For [Go](https://golang.org/doc/devel/release.html#policy) that is the two most recent major releases and for [PostgreSQL](https://www.postgresql.org/support/versioning/) the major releases in the last 5 years. This means pgx supports Go 1.16 and higher and PostgreSQL 10 and higher.

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. `v4` is the latest stable major version.

## PGX Family Libraries

pgx is the head of a family of PostgreSQL libraries. Many of these can be used independently. Many can also be accessed
from pgx for lower-level control.

### [github.com/jackc/pgconn](https://github.com/jackc/pgconn)

`pgconn` is a lower-level PostgreSQL database driver that operates at nearly the same level as the C library `libpq`.

### [github.com/yugabyte/pgx/v4/pgxpool](https://github.com/yugabyte/pgx/tree/master/pgxpool)

`pgxpool` is a connection pool for pgx. pgx is entirely decoupled from its default pool implementation. This means that pgx can be used with a different pool or without any pool at all.

### [github.com/yugabyte/pgx/v4/stdlib](https://github.com/yugabyte/pgx/tree/master/stdlib)

This is a `database/sql` compatibility layer for pgx. pgx can be used as a normal `database/sql` driver, but at any time, the native interface can be acquired for more performance or PostgreSQL specific functionality.

### [github.com/jackc/pgtype](https://github.com/jackc/pgtype)

Over 70 PostgreSQL types are supported including `uuid`, `hstore`, `json`, `bytea`, `numeric`, `interval`, `inet`, and arrays. These types support `database/sql` interfaces and are usable outside of pgx. They are fully tested in pgx and pq. They also support a higher performance interface when used with the pgx driver.

### [github.com/jackc/pgproto3](https://github.com/jackc/pgproto3)

pgproto3 provides standalone encoding and decoding of the PostgreSQL v3 wire protocol. This is useful for implementing very low level PostgreSQL tooling.

### [github.com/jackc/pglogrepl](https://github.com/jackc/pglogrepl)

pglogrepl provides functionality to act as a client for PostgreSQL logical replication.

### [github.com/jackc/pgmock](https://github.com/jackc/pgmock)

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

### [github.com/jackc/tern](https://github.com/jackc/tern)

tern is a stand-alone SQL migration system.

### [github.com/jackc/pgerrcode](https://github.com/jackc/pgerrcode)

pgerrcode contains constants for the PostgreSQL error codes.

## 3rd Party Libraries with PGX Support

### [github.com/georgysavva/scany](https://github.com/georgysavva/scany)

Library for scanning data from a database into Go structs and more.
