package mysql_size_summary

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type Mysql struct {
	Servers           []string `toml:"servers"`
	GatherPerDatabase bool     `toml:"per_database"`
	tls.ClientConfig
}

var sampleConfig = `
  ## specify servers via a url matching:
  ##  [username[:password]@][protocol[(address)]]/[?tls=[true|false|skip-verify|custom]]
  ##  see https://github.com/go-sql-driver/mysql#dsn-data-source-name
  ##  e.g.
  ##    servers = ["user:passwd@tcp(127.0.0.1:3306)/?tls=false"]
  ##    servers = ["user@tcp(127.0.0.1:3306)/?tls=false"]
  #
  ## If no servers are specified, then localhost is used as the host.
  servers = ["tcp(127.0.0.1:3306)/"]
  #
  ## gather 
  per_database                       = true

  ## Optional TLS Config (will be used if tls=custom parameter specified in server uri)
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`

var defaultTimeout = time.Second * time.Duration(5)

func (m *Mysql) SampleConfig() string {
	return sampleConfig
}

func (m *Mysql) Description() string {
	return "Read metrics about size of mysql database from one or many mysql servers"
}

var (
	localhost        = ""
	lastT            time.Time
	scanIntervalSlow uint32
)

func (m *Mysql) Gather(acc telegraf.Accumulator) error {
	if len(m.Servers) == 0 {
		// default to localhost if nothing specified.
		return m.gatherServer(localhost, acc)
	}

	tlsConfig, err := m.ClientConfig.TLSConfig()
	if err != nil {
		return fmt.Errorf("registering TLS config: %s", err)
	}

	if tlsConfig != nil {
		mysql.RegisterTLSConfig("custom", tlsConfig)
	}

	var wg sync.WaitGroup

	// Loop through each server and collect metrics
	for _, server := range m.Servers {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			acc.AddError(m.gatherServer(s, acc))
		}(server)
	}

	wg.Wait()
	return nil
}

// metric queries
const (
	totalDatabaseSizeQuery = `SELECT '' AS table_schema,SUM(data_length) AS data_size,
SUM(max_data_length) AS max_data_size,
SUM(data_free) AS data_free,
SUM(index_length) AS index_size
FROM information_schema.tables;`
	perDatabaseSizeQuery = `SELECT table_schema, SUM(data_length) AS data_size,
SUM(max_data_length) AS max_data_size,
SUM(data_free) AS data_free,
SUM(index_length) AS index_size
FROM information_schema.tables GROUP BY table_schema;`
)

func (m *Mysql) gatherServer(serv string, acc telegraf.Accumulator) error {
	serv, err := dsnAddTimeout(serv)
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", serv)
	if err != nil {
		return err
	}

	defer db.Close()
	return m.gatherDatabaseSize(db, serv, acc)
}

func (m *Mysql) gatherDatabaseSize(db *sql.DB, serv string, acc telegraf.Accumulator) error {
	servtag := getDSNTag(serv)
	var sql string
	if m.GatherPerDatabase {
		sql = perDatabaseSizeQuery
	} else {
		sql = totalDatabaseSizeQuery
	}
	rows, err := db.Query(sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			tableSchema   string
			dataLength    float64
			maxDataLength float64
			dataFree      float64
			indexLength   float64
		)
		err = rows.Scan(
			&tableSchema,
			&dataLength,
			&maxDataLength,
			&dataFree,
			&indexLength,
		)
		if err != nil {
			return err
		}
		tags := map[string]string{"server": servtag}
		if m.GatherPerDatabase {
			tags["database"] = tableSchema
		} else {
			tags["total"] = "true"
		}
		acc.AddFields("mysql_size", map[string]interface{}{
			"data_length":     dataLength,
			"max_data_length": maxDataLength,
			"data_free":       dataFree,
			"index_length":    indexLength,
		}, tags)
	}
	return nil
}

func dsnAddTimeout(dsn string) (string, error) {
	conf, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	if conf.Timeout == 0 {
		conf.Timeout = time.Second * 5
	}

	return conf.FormatDSN(), nil
}

func getDSNTag(dsn string) string {
	conf, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "127.0.0.1:3306"
	}
	return conf.Addr
}

func init() {
	inputs.Add("mysql_size_summary", func() telegraf.Input {
		return &Mysql{}
	})
}
