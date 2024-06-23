package otel

import (
	"go.opentelemetry.io/otel/metric"
	"sync"
)

// "init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"
// Following Metric Names will get instrumented as part of StateLogMetrics
const (
	// Worker States
	InitConnCountMetric      = "init_connection.count"
	AccptConnCountMetric     = "accept_connection.count"
	WaitConnCountMetric      = "wait_connection.count"
	BusyConnCountMetric      = "busy_connection.count"
	ScheduledConnCountMetric = "scheduled_connection.count"
	FinishedConnCountMetric  = "finished_connection.count"
	QuiescedConnCountMetric  = "quiesced_connection.count"

	// Connection States
	AssignedConnCountMetric = "assigned_connection.count"
	IdleConnCountMetric     = "idle_connection.count"
	BacklogConnCountMetric  = "backlog_connection.count"
	StrdConnCountMetric     = "stranded_connection.count"

	//Max value within window
	InitConnCountMetricMax      = "init_connection.count.max"
	AccptConnCountMetricMax     = "accept_connection.count.max"
	WaitConnCountMetricMax      = "wait_connection.count.max"
	BusyConnCountMetricMax      = "busy_connection.count.max"
	ScheduledConnCountMetricMax = "scheduled_connection.count.max"
	FinishedConnCountMetricMax  = "finished_connection.count.max"
	QuiescedConnCountMetricMax  = "quiesced_connection.count.max"

	// Connection States
	AssignedConnCountMetricMax = "assigned_connection.count.max"
	IdleConnCountMetricMax     = "idle_connection.count.max"
	BacklogConnCountMetricMax  = "backlog_connection.count.max"
	StrdConnCountMetricMax     = "stranded_connection.count.max"

	//Worker Request Response Count metrics suffix names
	WorkerRequestCountMetric  = "worker.req.count"
	WorkerResponseCountMetric = "worker.resp.count"
)

const (
	Target      = string("target")
	Endpoint    = string("target_ip_port")
	TLS_version = string("tls_version")
	Application = string("Application")
	ShardId     = string("ShardId")
	WorkerType  = string("WorkerType")
	InstanceId  = string("InstanceId")
	Datapoints  = string("datapoints")
)

const OtelInstrumentationVersion string = "v1.0"

// default OTEL configurations point to QA collector
const DEFAULT_OTEL_COLLECTOR_PROTOCOL string = "grpc"
const DEFAULT_OTEL_COLLECTOR__IP string = "0.0.0.0"
const DEFAULT_GRPC_OTEL_COLLECTOR_PORT string = "4317"
const DEFAULT_HTTP_OTEL_COLLECTOR_PORT string = "4318"
const COLLECTOR_POLLING_INTERVAL_SECONDS int32 = 5

const StateLogMeterName = "occ-statelog-data"

//****************************** variables ***************************

type Tags struct {
	TagName  string
	TagValue string
}

type WorkersStateData struct {
	ShardId    int
	WorkerType int
	InstanceId int
	StateData  map[string]int64
}

type (
	ServerType int
)

// StateData Represents stats by a worker
type StateData struct {
	Name       string
	Value      float64
	Dimensions metric.MeasurementOption
}

type DataPoint struct {
	attr metric.MeasurementOption
	data int64
}

// StateLogMetrics state_log_metrics reports workers states
type StateLogMetrics struct {

	//Statelog metrics configuration data
	metricsConfig stateLogMetricsConfig

	meter metric.Meter

	//Channel to receive statelog data
	mStateDataChan chan *WorkersStateData

	//Channel to close sending data
	doneCh chan struct{}

	stateLock sync.Mutex

	registration metric.Registration

	initState metric.Int64ObservableGauge
	acptState metric.Int64ObservableGauge
	waitState metric.Int64ObservableGauge
	busyState metric.Int64ObservableGauge
	schdState metric.Int64ObservableGauge
	fnshState metric.Int64ObservableGauge
	quceState metric.Int64ObservableGauge
	asgnState metric.Int64ObservableGauge
	idleState metric.Int64ObservableGauge
	bklgState metric.Int64ObservableGauge
	strdState metric.Int64ObservableGauge

	/*	initStateMax metric.Int64ObservableGauge
		acptStateMax metric.Int64ObservableGauge
		waitStateMax metric.Int64ObservableGauge
		busyStateMax metric.Int64ObservableGauge
		schdStateMax metric.Int64ObservableGauge
		fnshStateMax metric.Int64ObservableGauge
		quceStateMax metric.Int64ObservableGauge
		asgnStateMax metric.Int64ObservableGauge
		idleStateMax metric.Int64ObservableGauge
		bklgStateMax metric.Int64ObservableGauge
		strdStateMax metric.Int64ObservableGauge

		workerReqCount  metric.Int64ObservableUpDownCounter
		workerRespCount metric.Int64ObservableUpDownCounter*/
}

// Object represents the workers states data for worker belongs to specific shardId and workperType with flat-map
// between statename vs count.
type stateLogMetricsConfig struct {
	// MeterProvider sets the metric.MeterProvider.  If nil, the global
	// Provider will be used.
	MeterProvider metric.MeterProvider
	appName       string
}

// MetricProviderOption Define confuration for metric Provider Option
type MetricProviderOption struct {
	metric.MeterProvider
}

// StateLogOption Option Interface define configuration parameters for statelog metrics agent
type StateLogOption interface {
	apply(*stateLogMetricsConfig)
}

// AppNameOption Define Option for OCCName
type AppNameOption string

// Headers
const IngestTokenHeader = "X-Sf-Token"
