package mppostgres

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	// PostgreSQL Driver
	_ "github.com/lib/pq"
	mp "github.com/mackerelio/go-mackerel-plugin-helper"
	"github.com/mackerelio/golib/logging"
)

var logger = logging.GetLogger("metrics.plugin.postgres")

// PostgresPlugin mackerel plugin for PostgreSQL
type PostgresPlugin struct {
	Host     string
	Port     string
	Username string
	Password string
	SSLmode  string
	Prefix   string
	Timeout  int
	Tempfile string
	Option   string
}

func fetchCount(db *sqlx.DB) (map[string]interface{}, error) {
	query := `select count(*) from sample`

	rows, err := db.Query(query)
	if err != nil {
		logger.Errorf("Failed to select. %s", err)
		return nil, err
	}

	stat := map[string]interface{}{
		"count": 0,
	}

	for rows.Next() {
		var count float64
		if err := rows.Scan(&count); err != nil {
			logger.Warningf("Failed to scan %s", err)
			continue
		}
		stat["count"] = count
	}

	return stat, nil
}

func fetchSum(db *sqlx.DB) (map[string]interface{}, error) {
	query := `select sum(column2) from sample`

	rows, err := db.Query(query)
	if err != nil {
		logger.Errorf("Failed to select. %s", err)
		return nil, err
	}

	stat := map[string]interface{}{
		"sum": 0,
	}

	for rows.Next() {
		var count float64
		if err := rows.Scan(&count); err != nil {
			logger.Warningf("Failed to scan %s", err)
			continue
		}
		stat["sum"] = count
	}

	return stat, nil
}

func mergeStat(dst, src map[string]interface{}) {
	for k, v := range src {
		dst[k] = v
	}
}

// MetricKeyPrefix returns the metrics key prefix
func (p PostgresPlugin) MetricKeyPrefix() string {
	if p.Prefix == "" {
		p.Prefix = "postgres"
	}
	return p.Prefix
}

// FetchMetrics interface for mackerelplugin
func (p PostgresPlugin) FetchMetrics() (map[string]interface{}, error) {

	cmd := fmt.Sprintf("user=%s host=%s port=%s sslmode=%s connect_timeout=%d %s", p.Username, p.Host, p.Port, p.SSLmode, p.Timeout, p.Option)
	if p.Password != "" {
		cmd = fmt.Sprintf("password=%s %s", p.Password, cmd)
	}
	db, err := sqlx.Connect("postgres", cmd)

	if err != nil {
		logger.Errorf("FetchMetrics: %s", err)
		return nil, err
	}
	defer db.Close()

	statCount, err := fetchCount(db)
	if err != nil {
		return nil, err
	}
	statSum, err := fetchSum(db)
	if err != nil {
		return nil, err
	}

	stat := make(map[string]interface{})
	mergeStat(stat, statCount)
	mergeStat(stat, statSum)

	return stat, err
}

// GraphDefinition interface for mackerelplugin
func (p PostgresPlugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := strings.Title(p.MetricKeyPrefix())

	var graphdef = map[string]mp.Graphs{
		"Count": {
			Label: (labelPrefix + " Count"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "count", Label: "Count"},
			},
		},
		"Sum": {
			Label: (labelPrefix + " Sum"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "sum", Label: "Count"},
			},
		},
	}

	return graphdef
}

// Do the plugin
func Do() {
	optHost := flag.String("hostname", "localhost", "Hostname to login to")
	optPort := flag.String("port", "5432", "Database port")
	optUser := flag.String("user", "", "Postgres User")
	optDatabase := flag.String("database", "", "Database name")
	optPass := flag.String("password", os.Getenv("PGPASSWORD"), "Postgres Password")
	optPrefix := flag.String("metric-key-prefix", "postgres", "Metric key prefix")
	optSSLmode := flag.String("sslmode", "disable", "Whether or not to use SSL")
	optConnectTimeout := flag.Int("connect_timeout", 5, "Maximum wait for connection, in seconds.")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	flag.Parse()

	if *optUser == "" {
		logger.Warningf("user is required")
		flag.PrintDefaults()
		os.Exit(1)
	}
	option := ""
	if *optDatabase != "" {
		option = fmt.Sprintf("dbname=%s", *optDatabase)
	}

	var postgres PostgresPlugin
	postgres.Host = *optHost
	postgres.Port = *optPort
	postgres.Username = *optUser
	postgres.Password = *optPass
	postgres.Prefix = *optPrefix
	postgres.SSLmode = *optSSLmode
	postgres.Timeout = *optConnectTimeout
	postgres.Option = option

	helper := mp.NewMackerelPlugin(postgres)

	helper.Tempfile = *optTempfile
	helper.Run()
}
