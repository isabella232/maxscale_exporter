package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	envPrefix   = "MAXSCALE_EXPORTER"
	metricsPath = "/metrics"
	namespace   = "maxscale"
)

func init() {
	threadMetrics.Reads = newThreadDesc("reads", "Number of read events", prometheus.CounterValue)
	threadMetrics.Writes = newThreadDesc("writes", "Number of write events", prometheus.CounterValue)
	threadMetrics.Errors = newThreadDesc("errors", "Number of error events", prometheus.CounterValue)
	threadMetrics.Hangups = newThreadDesc("hangups", "Number of hangup events", prometheus.CounterValue)
	threadMetrics.Accepts = newThreadDesc("accepts", "Number of accept events", prometheus.CounterValue)

	allThreadMetricsDesc = []*Metric{threadMetrics.Reads, threadMetrics.Writes, threadMetrics.Errors,
		threadMetrics.Hangups, threadMetrics.Accepts}
}

type threadMetricsT struct {
	Reads   *Metric
	Writes  *Metric
	Errors  *Metric
	Hangups *Metric
	Accepts *Metric
}

type MaxScale struct {
	Address   string
	username  string
	password  string
	queryBase string

	up              prometheus.Gauge
	totalScrapes    prometheus.Counter
	serverMetrics   map[string]Metric
	serviceMetrics  map[string]Metric
	statusMetrics   map[string]Metric
	variableMetrics map[string]Metric
	eventMetrics    map[string]Metric
}

type Status struct {
	Name  string  `json:"Variable_name"`
	Value float64 `json:"Value"`
}

type Variable struct {
	Name  string      `json:"Variable_name"`
	Value json.Number `json:"Value,num_integer"`
}

type Event struct {
	Duration string `json:"Duration"`
	Queued   uint64 `json:"No. Events Queued"`
	Executed uint64 `json:"No. Events Executed"`
}

type Metric struct {
	Desc      *prometheus.Desc
	ValueType prometheus.ValueType
}

var (
	serverLabelNames    = []string{"server", "address"}
	serverUpLabelNames  = []string{"server", "address", "status"}
	serviceLabelNames   = []string{"name", "router"}
	threadLabelNames    = []string{"id"}
	statusLabelNames    = []string{}
	variablesLabelNames = []string{}
	eventLabelNames     = []string{}

	threadMetrics        threadMetricsT
	allThreadMetricsDesc []*Metric
)

type metrics map[string]Metric

func newThreadDesc(name string, help string, t prometheus.ValueType) *Metric {
	fqName := prometheus.BuildFQName(namespace, "thread", name)
	desc := prometheus.NewDesc(fqName, help, threadLabelNames, nil)
	return &Metric{desc, t}
}

func newDesc(subsystem string, name string, help string, variableLabels []string, t prometheus.ValueType) Metric {
	return Metric{
		prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, name),
			help, variableLabels, nil,
		), t}
}

var (
	serverMetrics = metrics{
		"server_connections": newDesc("server", "connections", "Amount of connections to the server", serverLabelNames, prometheus.GaugeValue),
		"server_up":          newDesc("server", "up", "Is the server up", serverUpLabelNames, prometheus.GaugeValue),
	}
	serviceMetrics = metrics{
		"service_current_sessions": newDesc("service", "current_sessions", "Amount of sessions currently active", serviceLabelNames, prometheus.GaugeValue),
		"service_sessions_total":   newDesc("service", "total_sessions", "Total amount of sessions", serviceLabelNames, prometheus.CounterValue),
	}

	statusMetrics = metrics{
		"status_uptime":                    newDesc("status", "uptime", "How long has the server been running", statusLabelNames, prometheus.CounterValue),
		"status_uptime_since_flush_status": newDesc("status", "uptime_since_flush_status", "How long the server has been up since flush status", statusLabelNames, prometheus.CounterValue),
		"status_threads_created":           newDesc("status", "threads_created", "How many threads have been created", statusLabelNames, prometheus.CounterValue),
		"status_threads_running":           newDesc("status", "threads_running", "How many threads are running", statusLabelNames, prometheus.GaugeValue),
		"status_threadpool_threads":        newDesc("status", "threadpool_threads", "How many threadpool threads there are", statusLabelNames, prometheus.GaugeValue),
		"status_threads_connected":         newDesc("status", "threads_connected", "How many threads are connected", statusLabelNames, prometheus.GaugeValue),
		"status_connections":               newDesc("status", "connections", "How many connections there are", statusLabelNames, prometheus.GaugeValue),
		"status_client_connections":        newDesc("status", "client_connections", "How many client connections there are", statusLabelNames, prometheus.GaugeValue),
		"status_backend_connections":       newDesc("status", "backend_connections", "How many backend connections there are", statusLabelNames, prometheus.GaugeValue),
		"status_listeners":                 newDesc("status", "listeners", "How many listeners there are", statusLabelNames, prometheus.GaugeValue),
		"status_zombie_connections":        newDesc("status", "zombie_connections", "How many zombie connetions there are", statusLabelNames, prometheus.GaugeValue),
		"status_internal_descriptors":      newDesc("status", "internal_descriptors", "How many internal descriptors there are", statusLabelNames, prometheus.GaugeValue),
		"status_read_events":               newDesc("status", "read_events", "How many read events happened", statusLabelNames, prometheus.CounterValue),
		"status_write_events":              newDesc("status", "write_events", "How many write events happened", statusLabelNames, prometheus.CounterValue),
		"status_hangup_events":             newDesc("status", "hangup_events", "How many hangup events happened", statusLabelNames, prometheus.CounterValue),
		"status_error_events":              newDesc("status", "error_events", "How many error events happened", statusLabelNames, prometheus.CounterValue),
		"status_accept_events":             newDesc("status", "accept_events", "How many accept events happened", statusLabelNames, prometheus.CounterValue),
		"status_event_queue_length":        newDesc("status", "event_queue_length", "How long the event queue is", statusLabelNames, prometheus.GaugeValue),
		"status_max_event_queue_length":    newDesc("status", "max_event_queue_length", "The max length of the event queue", statusLabelNames, prometheus.GaugeValue),
		"status_max_event_queue_time":      newDesc("status", "max_event_queue_time", "The max event queue time", statusLabelNames, prometheus.GaugeValue),
		"status_max_event_execution_time":  newDesc("status", "max_event_execution_time", "The max event execution time", statusLabelNames, prometheus.GaugeValue),
		"status_pending_events":            newDesc("status", "pending_events", "How many events are pending", statusLabelNames, prometheus.GaugeValue),
	}

	variableMetrics = metrics{
		"variables_maxscale_threads":   newDesc("variables", "thread", "MAXSCALE_THREADS", variablesLabelNames, prometheus.GaugeValue),
		"variables_maxscale_nbpolls":   newDesc("variables", "nbpolls", "MAXSCALE_NBPOLLS", variablesLabelNames, prometheus.GaugeValue),
		"variables_maxscale_pollsleep": newDesc("variables", "pollsleep", "MAXSCALE_POLLSLEEP", variablesLabelNames, prometheus.GaugeValue),
		"variables_maxscale_sessions":  newDesc("variables", "sessions", "MAXSCALE_SESSIONS", variablesLabelNames, prometheus.GaugeValue),
	}

	eventMetrics = metrics{
		// Histograms don't have ValueType's, so use the UntypedValue instead
		"events_queued_seconds":   newDesc("events", "queued_seconds", "Amount of events queued", eventLabelNames, prometheus.UntypedValue),
		"events_executed_seconds": newDesc("events", "executed_seconds", "Amount of events executed", eventLabelNames, prometheus.UntypedValue),
	}
)

func NewExporter(address string, username string, password string) (*MaxScale, error) {
	return &MaxScale{
		Address:   address,
		username:  username,
		password:  password,
		queryBase: "http://" + username + ":" + password + "@" + address + "/v1/",

		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Was the last scrape of MaxScale successful?",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_total_scrapes",
			Help:      "Current total MaxScale scrapes",
		}),
		serverMetrics:   serverMetrics,
		serviceMetrics:  serviceMetrics,
		statusMetrics:   statusMetrics,
		variableMetrics: variableMetrics,
		eventMetrics:    eventMetrics,
	}, nil
}

// Describe describes all the metrics ever exported by the MaxScale exporter. It
// implements prometheus.Collector.
func (m *MaxScale) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range m.eventMetrics {
		ch <- m.Desc
	}

	for _, m := range m.variableMetrics {
		ch <- m.Desc
	}

	for _, m := range m.statusMetrics {
		ch <- m.Desc
	}

	for _, m := range m.serviceMetrics {
		ch <- m.Desc
	}

	for _, m := range m.serverMetrics {
		ch <- m.Desc
	}

	for _, md := range allThreadMetricsDesc {
		ch <- md.Desc
	}

	ch <- m.up.Desc()
	ch <- m.totalScrapes.Desc()
}

// Collect fetches the stats from configured MaxScale location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (m *MaxScale) Collect(ch chan<- prometheus.Metric) {
	m.totalScrapes.Inc()

	var parseErrors = false
	if err := m.parseServers(ch); err != nil {
		parseErrors = true
		log.Print(err)
	}

	if err := m.parseServices(ch); err != nil {
		parseErrors = true
		log.Print(err)
	}

	if res, err := m.parseThreads(); err == nil {
		for _, e := range res {
			ch <- e
		}
	} else {
		parseErrors = true
		log.Print(err)
	}

	// if err := m.parseStatus(ch); err != nil {
	// 	parseErrors = true
	// 	log.Print(err)
	// }
	//
	// if err := m.parseVariables(ch); err != nil {
	// 	parseErrors = true
	// 	log.Print(err)
	// }
	//
	// if err := m.parseEvents(ch); err != nil {
	// 	parseErrors = true
	// 	log.Print(err)
	// }

	if parseErrors {
		m.up.Set(0)
	} else {
		m.up.Set(1)
	}
	ch <- m.up
	ch <- m.totalScrapes
}

func (m *MaxScale) getStatistics(path string, v interface{}) error {
	query := m.queryBase + path

	resp, err := http.Get(query)
	if err != nil {
		return fmt.Errorf("Error while getting %v: %v", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Status error: %v", resp.StatusCode)
	}

	jsonDataFromHttp, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Error while reading response from %v: %v", path, err)
	}

	data := bytes.Replace(jsonDataFromHttp, []byte("NULL"), []byte("null"), -1)
	return json.Unmarshal(data, v)
}

func serverUp(status string) float64 {
	if strings.Contains(status, ",Down,") {
		return 0
	}
	if strings.Contains(status, ",Running,") {
		return 1
	}
	return 0
}

func (m *MaxScale) parseServers(ch chan<- prometheus.Metric) error {
	var servers ServersData
	err := m.getStatistics("servers", &servers)

	if err != nil {
		return err
	}

	for _, serverdata := range servers.Data {
		connectionsMetric := m.serverMetrics["server_connections"]
		ch <- prometheus.MustNewConstMetric(
			connectionsMetric.Desc,
			connectionsMetric.ValueType,
			serverdata.Attributes.Statistics.Connections,
			serverdata.ID, serverdata.Attributes.Parameters.Address,
		)

		// We surround the separated list with the separator as well. This way regular expressions
		// in labeling don't have to consider satus positions.
		normalizedStatus := "," + strings.Replace(serverdata.Attributes.State, ", ", ",", -1) + ","

		upMetric := m.serverMetrics["server_up"]
		ch <- prometheus.MustNewConstMetric(
			upMetric.Desc,
			upMetric.ValueType,
			serverUp(normalizedStatus),
			serverdata.ID, serverdata.Attributes.Parameters.Address, normalizedStatus,
		)
	}

	return nil
}

func (m *MaxScale) parseServices(ch chan<- prometheus.Metric) error {
	var services ServicesData
	err := m.getStatistics("services", &services)

	if err != nil {
		return err
	}

	for _, service := range services.Data {
		currentSessions := m.serviceMetrics["service_current_sessions"]
		ch <- prometheus.MustNewConstMetric(
			currentSessions.Desc, currentSessions.ValueType,
			service.Attributes.Statistics.Connections,
			service.ID, service.Attributes.Router,
		)

		totalSessions := m.serviceMetrics["service_sessions_total"]
		ch <- prometheus.MustNewConstMetric(
			totalSessions.Desc, totalSessions.ValueType,
			service.Attributes.Statistics.TotalConnections,
			service.ID, service.Attributes.Router,
		)
	}

	return nil
}

func (m *MaxScale) parseThreads() ([]prometheus.Metric, error) {
	var threads ThreadsData
	err := m.getStatistics("maxscale/threads", &threads)

	if err != nil {
		return nil, err
	}

	results := make([]prometheus.Metric, 0, 100)
	addMetric := func(metricDesc *Metric, value float64, id string) {
		newMetric := prometheus.MustNewConstMetric(metricDesc.Desc, metricDesc.ValueType, value, id)
		results = append(results, newMetric)
	}

	for _, element := range threads.Data {
		id := element.ID
		stats := &element.Attributes.Stats
		addMetric(threadMetrics.Reads, stats.Reads, id)
		addMetric(threadMetrics.Writes, stats.Writes, id)
		addMetric(threadMetrics.Errors, stats.Errors, id)
		addMetric(threadMetrics.Hangups, stats.Hangups, id)
		addMetric(threadMetrics.Accepts, stats.Accepts, id)
	}
	return results, nil
}

func (m *MaxScale) parseStatus(ch chan<- prometheus.Metric) error {
	var status []Status
	err := m.getStatistics("/status", &status)

	if err != nil {
		return err
	}

	for _, element := range status {
		metricName := "status_" + strings.ToLower(element.Name)
		metric := m.statusMetrics[metricName]
		ch <- prometheus.MustNewConstMetric(
			metric.Desc,
			metric.ValueType,
			element.Value,
		)
	}

	return nil
}
func (m *MaxScale) parseVariables(ch chan<- prometheus.Metric) error {
	var variables []Variable
	err := m.getStatistics("/variables", &variables)

	if err != nil {
		return err
	}

	for _, element := range variables {
		metricName := "variables_" + strings.ToLower(element.Name)
		if _, ok := m.variableMetrics[metricName]; ok {
			value, err := element.Value.Float64()
			if err != nil {
				return err
			}
			metric := m.variableMetrics[metricName]
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.ValueType,
				value,
			)
		}
	}

	return nil
}
func (m *MaxScale) parseEvents(ch chan<- prometheus.Metric) error {
	var events []Event
	err := m.getStatistics("/event/times", &events)

	if err != nil {
		return err
	}

	eventExecutedBuckets := map[float64]uint64{
		0.1: 0,
		0.2: 0,
		0.3: 0,
		0.4: 0,
		0.5: 0,
		0.6: 0,
		0.7: 0,
		0.8: 0,
		0.9: 0,
		1.0: 0,
		1.1: 0,
		1.2: 0,
		1.3: 0,
		1.4: 0,
		1.5: 0,
		1.6: 0,
		1.7: 0,
		1.8: 0,
		1.9: 0,
		2.0: 0,
		2.1: 0,
		2.2: 0,
		2.3: 0,
		2.4: 0,
		2.5: 0,
		2.6: 0,
		2.7: 0,
		2.8: 0,
		2.9: 0,
	}
	executedSum := float64(0)
	executedCount := uint64(0)
	executedTime := 0.1
	for _, element := range events {
		executedCount += element.Executed
		executedSum = executedSum + (float64(element.Executed) * executedTime)
		executedTime += 0.1
		switch element.Duration {
		case "< 100ms":
			eventExecutedBuckets[0.1] = element.Executed
		case "> 3000ms":
			// Do nothing as these will get accumulated in the +Inf bucket
		default:
			durationf := strings.Split(element.Duration, " ")
			ad := strings.Trim(durationf[len(durationf)-1], "ms")
			milliseconds, _ := strconv.ParseFloat(ad, 64)
			seconds := milliseconds / 1000
			eventExecutedBuckets[seconds] = element.Executed
		}
	}

	desc := prometheus.NewDesc(
		"maxscale_events_executed_seconds",
		"Amount of events executed",
		[]string{},
		prometheus.Labels{},
	)

	// Create a constant histogram from values we got from a 3rd party telemetry system.
	ch <- prometheus.MustNewConstHistogram(
		desc,
		executedCount, executedSum,
		eventExecutedBuckets,
	)

	eventQueuedBuckets := map[float64]uint64{
		0.1: 0,
		0.2: 0,
		0.3: 0,
		0.4: 0,
		0.5: 0,
		0.6: 0,
		0.7: 0,
		0.8: 0,
		0.9: 0,
		1.0: 0,
		1.1: 0,
		1.2: 0,
		1.3: 0,
		1.4: 0,
		1.5: 0,
		1.6: 0,
		1.7: 0,
		1.8: 0,
		1.9: 0,
		2.0: 0,
		2.1: 0,
		2.2: 0,
		2.3: 0,
		2.4: 0,
		2.5: 0,
		2.6: 0,
		2.7: 0,
		2.8: 0,
		2.9: 0,
	}

	queuedSum := float64(0)
	queuedCount := uint64(0)
	queuedTime := 0.1
	for _, element := range events {
		queuedCount += element.Queued
		queuedSum = queuedSum + (float64(element.Queued) * queuedTime)
		queuedTime += 0.1
		switch element.Duration {
		case "< 100ms":
			eventQueuedBuckets[0.1] = element.Queued
		case "> 3000ms":
			// Do nothing as this gets accumulated in the +Inf bucket
		default:
			durationf := strings.Split(element.Duration, " ")
			ad := strings.Trim(durationf[len(durationf)-1], "ms")
			milliseconds, _ := strconv.ParseFloat(ad, 64)
			seconds := milliseconds / 1000
			eventQueuedBuckets[seconds] = element.Queued
		}
	}

	queuedDesc := prometheus.NewDesc(
		"maxscale_events_queued_seconds",
		"Amount of events queued",
		[]string{},
		prometheus.Labels{},
	)

	// Create a constant histogram from values we got from a 3rd party telemetry system.
	ch <- prometheus.MustNewConstHistogram(
		queuedDesc,
		queuedCount, queuedSum,
		eventQueuedBuckets,
	)

	return nil
}

// stringEnvVar is like flag.StringVar, with default value overridden by an environment
// variable when present. E.g. with address, the env var used
// is MAXSCALE_EXPORTER_ADDRESS.
func stringEnvVar(output *string, name string, defaultVal string, usage string) {
	if envVal, ok := os.LookupEnv(envPrefix + strings.ToUpper(name)); ok {
		flag.StringVar(output, name, envVal, usage)
	}
	flag.StringVar(output, name, defaultVal, usage)
}

func main() {
	log.SetFlags(0)

	var address, restUser, restPassword, listenPort, pidfile string
	stringEnvVar(&address, "address", "127.0.0.1:8989", "address:port of MaxScale REST API")
	stringEnvVar(&listenPort, "port", "9195", "the port that the maxscale exporter listens on")
	stringEnvVar(&pidfile, "pidfile", "", "the pid file for maxscale to monitor process statistics")
	stringEnvVar(&restUser, "restuser", "admin", "MaxScale REST-api username")
	stringEnvVar(&restPassword, "restpw", "mariadb", "MaxScale REST-api password")
	flag.Parse()

	log.Printf("Starting MaxScale exporter, scraping MaxScale REST API at: %v", address)
	exporter, err := NewExporter(address, restUser, restPassword)
	if err != nil {
		log.Fatalf("Failed to start maxscale exporter: %v\n", err)
	}

	if pidfile != "" {
		log.Printf("Parsing PID file located at %v", pidfile)
		procExporter := prometheus.NewProcessCollectorPIDFn(
			func() (int, error) {
				content, err := ioutil.ReadFile(pidfile)
				if err != nil {
					log.Printf("Can't read PID file: %s", err)
					return 0, fmt.Errorf("Can't read pid file: %s", err)
				}
				value, err := strconv.Atoi(strings.TrimSpace(string(content)))
				if err != nil {
					log.Printf("Can't parse PID file: %s", err)
					return 0, fmt.Errorf("Can't parse pid file: %s", err)
				}
				return value, nil
			}, namespace)
		prometheus.MustRegister(procExporter)
	}

	prometheus.MustRegister(exporter)
	http.Handle(metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(
			`<!DOCTYPE html>
<html>
<head><title>MaxScale Exporter</title></head>
<body>
<h1>MaxScale Exporter</h1>
<p><a href="` + metricsPath + `">Metrics</a></p>
</body>
</html>`))
	})
	log.Printf("Started MaxScale exporter, listening on port: %v", listenPort)
	log.Fatal(http.ListenAndServe(":"+listenPort, nil))

}
