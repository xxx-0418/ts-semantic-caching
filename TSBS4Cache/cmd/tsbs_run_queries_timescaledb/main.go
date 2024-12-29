// tsbs_run_queries_timescaledb speed tests TimescaleDB using requests from stdin or file
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the provided PostgreSQL/TimescaleDB endpoint.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/blagojts/viper"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/TimescaleDB_Client/timescaledb_client"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"regexp"
	"strings"
	"time"
)

const pgxDriver = "pgx"
const pqDriver = "postgres"

var (
	postgresConnect string
	hostList        []string
	user            string
	pass            string
	port            string
	showExplain     bool
	forceTextFormat bool
)

var totalRowLength int64 = 0
var (
	daemonUrls []string
	DBConns    []*sql.DB
)

var (
	runner *query.BenchmarkRunner
	driver string
)

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("postgres", "host=postgres user=postgres sslmode=disable",
		"String of additional PostgreSQL connection parameters, e.g., 'sslmode=disable'. Parameters for host and database will be ignored.")
	pflag.String("hosts", "localhost", "Comma separated list of PostgreSQL hosts (pass multiple values for sharding reads on a multi-node setup)")
	pflag.String("user", "postgres", "User to connect to PostgreSQL as")
	pflag.String("pass", "", "Password for the user connecting to PostgreSQL (leave blank if not password protected)")
	pflag.String("port", "5432", "Which port to connect to on the database host")

	pflag.Bool("show-explain", false, "Print out the EXPLAIN output for sample query")
	pflag.Bool("force-text-format", false, "Send/receive data in text format")

	timescaledb_client.DbName = constants.FormatTimescaleDB

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	postgresConnect = viper.GetString("postgres")
	hosts := viper.GetString("hosts")
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	port = viper.GetString("port")
	showExplain = viper.GetBool("show-explain")
	forceTextFormat = viper.GetBool("force-text-format")

	if showExplain {
		runner.SetLimit(1)
	}

	if forceTextFormat {
		driver = pqDriver
	} else {
		driver = pgxDriver
	}

	for _, host := range strings.Split(hosts, ",") {
		hostList = append(hostList, host)
	}

	timescaledb_client.DB = viper.GetString("db-name")

	runner = query.NewBenchmarkRunner(config)

	DBConns = make([]*sql.DB, len(hostList))
	for i := range hostList {
		DBConns[i], _ = sql.Open(driver, getConnectString(i))
	}
}

func main() {
	runner.Run(&query.TimescaleDBPool, newProcessor)

	fmt.Println("\navg row num per query result: ", totalRowLength/200000)
}

func getConnectString(workerNumber int) string {
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := re.ReplaceAllString(postgresConnect, "")
	host := hostList[workerNumber%len(hostList)]
	connectString = fmt.Sprintf("host=%s dbname=%s user=%s %s", host, runner.DatabaseName(), user, connectString)
	if len(port) > 0 {
		connectString = fmt.Sprintf("%s port=%s", connectString, port)
	}
	if len(pass) > 0 {
		connectString = fmt.Sprintf("%s password=%s", connectString, pass)
	}
	if forceTextFormat {
		connectString = fmt.Sprintf("%s disable_prepared_binary_result=yes binary_parameters=no", connectString)
	}

	return connectString
}

func prettyPrintResponse(rows *sql.Rows, q *query.TimescaleDB) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)
	resp["results"] = mapRows(rows)

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(r *sql.Rows) []map[string]interface{} {
	rows := []map[string]interface{}{}
	cols, _ := r.Columns()
	for r.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		err := r.Scan(values...)
		if err != nil {
			panic(errors.Wrap(err, "error while reading values"))
		}

		for i, column := range cols {
			row[column] = *values[i].(*interface{})
		}
		rows = append(rows, row)
	}
	return rows
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	db        *sql.DB
	opts      *queryExecutorOptions
	workerNum int
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	db, err := sql.Open(driver, getConnectString(workerNumber))
	if err != nil {
		panic(err)
	}
	p.db = db
	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
	p.workerNum = workerNumber
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.TimescaleDB)

	byteLength := uint64(0)
	hitKind := uint8(0)

	qry := string(tq.SqlQuery)
	sss := strings.Split(qry, ";")
	queryString := sss[0]
	segment := ""
	if len(sss) == 2 {
		segment = sss[1]
	}

	start := time.Now()

	if strings.EqualFold(timescaledb_client.UseCache, "stscache") {
		_, byteLength, hitKind = timescaledb_client.STsCacheClientSeg(DBConns[p.workerNum%len(DBConns)], queryString, segment)
	} else {
		rows, err := DBConns[p.workerNum%len(DBConns)].Query(queryString)
		intervalLen := 0
		if err != nil {
			return nil, err
		}
		if p.opts.printResponse {
			for rows.Next() {
				intervalLen++
			}
			fmt.Println("data row length: ", intervalLen)
		}
		totalRowLength += int64(intervalLen)
		for rows.Next() {
		}
		rows.Close()
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.InitWithParam(q.HumanLabelName(), took, byteLength, hitKind)
	var err error
	return []*query.Stat{stat}, err
}
