package otel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/paypal/hera/utility/logger"
	"github.com/paypal/hera/utility/logger/otel/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"io"
	"os"
	"strings"
	"time"
)

// InitializeOTelSDK SetupOTelSDK bootstrap the OTEL SDK pipeline initialization
func InitializeOTelSDK(ctx context.Context) (shutdown func(ctx context.Context) error, err error) {

	var shutdownFuncs []func(context.Context) error

	//shutdown calls cleanup function registered via shutdown functions
	//The errors from calls are joined
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			if fnErr := fn(ctx); fnErr != nil {
				err = errors.Join(err, fnErr) // You can use other error accumulation strategies if needed
			}
		}
		shutdownFuncs = nil
		return err
	}

	//handle error calls shutdownfor cleanup and make sure all errors returhed
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	//Initialize trace provider
	traceProvider, err := newTraceProvider(ctx)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, traceProvider.Shutdown)
	otel.SetTracerProvider(traceProvider)

	//Setup meter provider
	meterProvider, err := newMeterProvider(ctx)
	otel.SetMeterProvider(meterProvider)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	return
}

func newTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {

	traceExporter, err := getTraceExporter(ctx)
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter,
			trace.WithBatchTimeout(5*time.Second),
			trace.WithExportTimeout(2*time.Second),
			trace.WithMaxExportBatchSize(10),
			trace.WithMaxQueueSize(10),
		),
		// Default is 5s. Set to 1s for demonstrative purposes.
		trace.WithResource(getResourceInfo(config.OTelConfigData.PoolName)),
	)
	return traceProvider, nil
}

// Initialize newMeterProvider respective exporter either HTTP or GRPC exporter
func newMeterProvider(ctx context.Context) (*metric.MeterProvider, error) {
	metricExporter, err := getMetricExporter(ctx)

	if err != nil {
		logger.GetLogger().Log(logger.Alert, "failed to initialize metric exporter, error %v", err)
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(getResourceInfo(config.OTelConfigData.PoolName)),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			metric.WithInterval(time.Duration(config.OTelConfigData.ResolutionTimeInSec)*time.Second))),
	)
	return meterProvider, nil
}

// getMetricExporter Initialize metric exporter based protocol selected by user.
func getMetricExporter(ctx context.Context) (metric.Exporter, error) {
	if config.OTelConfigData.UseOtelGRPC {
		return newGRPCExporter(ctx)
	}
	return newHTTPExporter(ctx)
}

// getTraceExporter Initialize span exporter based protocol(GRPC or HTTP) selected by user.
func getTraceExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	if config.OTelConfigData.UseOtelGRPC {
		return newGRPCTraceExporter(ctx)
	}
	return newHTTPTraceExporter(ctx)
}

// newHTTPExporter Initilizes The "otlpmetrichttp" exporter in OpenTelemetry is used to export metrics data using the
// OpenTelemetry Protocol (OTLP) over HTTP.
func newHTTPExporter(ctx context.Context) (metric.Exporter, error) {
	headers := make(map[string]string)
	headers[IngestTokenHeader] = config.GetOTelIngestToken()

	//Currently all metrics uses delta-temporality: Delta Temporality: Use when you are interested in the rate of change
	//over time or when you need to report only the differences (deltas) between measurements.
	//This is useful for metrics like CPU usage, request rates, or other metrics where the rate of change is important.
	var temporalitySelector = func(instrument metric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }

	return otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.HttpPort)),
		otlpmetrichttp.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
		otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
		otlpmetrichttp.WithTemporalitySelector(temporalitySelector),
		otlpmetrichttp.WithHeaders(headers),
		otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
			// Enabled indicates whether to not retry sending batches in case
			// of export failure.
			Enabled: false,
			// InitialInterval the time to wait after the first failure before
			// retrying.
			InitialInterval: 1 * time.Second,
			// MaxInterval is the upper bound on backoff interval. Once this
			// value is reached the delay between consecutive retries will
			// always be `MaxInterval`.
			MaxInterval: 10 * time.Second,
			// MaxElapsedTime is the maximum amount of time (including retries)
			// spent trying to send a request/batch. Once this value is
			// reached, the data is discarded.
			MaxElapsedTime: 20 * time.Second,
		}),
		otlpmetrichttp.WithURLPath(config.OTelConfigData.MetricsURLPath),
		otlpmetrichttp.WithInsecure(), //Since agent is local
	)
}

// newGRPCExporter Initializes The "otlpmetricgrpc" exporter in OpenTelemetry is used to export metrics data using the
// OpenTelemetry Protocol (OTLP) over GRPC.
func newGRPCExporter(ctx context.Context) (metric.Exporter, error) {

	headers := make(map[string]string)
	headers[IngestTokenHeader] = config.GetOTelIngestToken()

	//Currently all metrics uses delta-temporality: Delta Temporality: Use when you are interested in the rate of change
	//over time or when you need to report only the differences (deltas) between measurements.
	//This is useful for metrics like CPU usage, request rates, or other metrics where the rate of change is important.
	var temporalitySelector = func(instrument metric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }

	return otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.GRPCPort)),
		otlpmetricgrpc.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
		otlpmetricgrpc.WithHeaders(headers),
		otlpmetricgrpc.WithReconnectionPeriod(time.Duration(5)*time.Second),
		otlpmetricgrpc.WithTemporalitySelector(temporalitySelector),
		otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
			// Enabled indicates whether to not retry sending batches in case
			// of export failure.
			Enabled: false,
			// InitialInterval the time to wait after the first failure before
			// retrying.
			InitialInterval: 1 * time.Second,
			// MaxInterval is the upper bound on backoff interval. Once this
			// value is reached the delay between consecutive retries will
			// always be `MaxInterval`.
			MaxInterval: 10 * time.Second,
			// MaxElapsedTime is the maximum amount of time (including retries)
			// spent trying to send a request/batch. Once this value is
			// reached, the data is discarded.
			MaxElapsedTime: 20 * time.Second,
		}),
		otlpmetricgrpc.WithInsecure(), //Since agent is local
	)
}

// newHTTPTraceExporter Initilizes The "otlptracehttp" exporter in OpenTelemetry is used to export spans data using the
// OpenTelemetry Protocol (OTLP) over HTTP.
func newHTTPTraceExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	headers := make(map[string]string)
	headers[IngestTokenHeader] = config.GetOTelIngestToken()

	return otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.HttpPort)),
		otlptracehttp.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			// Enabled indicates whether to not retry sending batches in case
			// of export failure.
			Enabled: false,
			// InitialInterval the time to wait after the first failure before
			// retrying.
			InitialInterval: 1 * time.Second,
			// MaxInterval is the upper bound on backoff interval. Once this
			// value is reached the delay between consecutive retries will
			// always be `MaxInterval`.
			MaxInterval: 10 * time.Second,
			// MaxElapsedTime is the maximum amount of time (including retries)
			// spent trying to send a request/batch. Once this value is
			// reached, the data is discarded.
			MaxElapsedTime: 20 * time.Second,
		}),
		otlptracehttp.WithURLPath(config.OTelConfigData.TraceURLPath),
		otlptracehttp.WithInsecure(), //Since agent is local
	)
}

// newGRPCTraceExporter Initilizes The "otlptracegrpc" exporter in OpenTelemetry is used to export spans data using the
// OpenTelemetry Protocol (OTLP) over GRPC.
func newGRPCTraceExporter(ctx context.Context) (*otlptrace.Exporter, error) {

	headers := make(map[string]string)
	headers[IngestTokenHeader] = config.GetOTelIngestToken()

	return otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(fmt.Sprintf("%s:%d", config.OTelConfigData.Host, config.OTelConfigData.GRPCPort)),
		otlptracegrpc.WithTimeout(time.Duration(config.OTelConfigData.ExporterTimeout)*time.Second),
		otlptracegrpc.WithHeaders(headers),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			// Enabled indicates whether to not retry sending batches in case
			// of export failure.
			Enabled: false,
			// InitialInterval the time to wait after the first failure before
			// retrying.
			InitialInterval: 1 * time.Second,
			// MaxInterval is the upper bound on backoff interval. Once this
			// value is reached the delay between consecutive retries will
			// always be `MaxInterval`.
			MaxInterval: 10 * time.Second,
			// MaxElapsedTime is the maximum amount of time (including retries)
			// spent trying to send a request/batch. Once this value is
			// reached, the data is discarded.
			MaxElapsedTime: 20 * time.Second,
		}),
		otlptracegrpc.WithInsecure(), //Since agent is local
	)
}

// getEnvFromSyshieraYaml returns the env: line from /etc/syshiera.yaml
func getEnvFromSyshieraYaml() (string, string, error) {
	filePath := "/etc/syshiera.yaml"
	var colo string = "qa"
	var env string = config.OTelConfigData.Environment
	var az, dc string
	var err error
	file, err := os.Open(filePath)
	if err != nil {
		return colo, env, err
	}
	defer file.Close()
	fileReader := bufio.NewReader(file)
	scanner := bufio.NewScanner(fileReader)
	for scanner.Scan() {
		line := scanner.Text()
		err = scanner.Err()
		if err != nil {
			if err == io.EOF {
				break
			}
			return colo, env, err
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "pp_az:") {
			az = strings.TrimSpace(line[6:len(line)])
		} else if strings.HasPrefix(line, "dc:") {
			dc = strings.TrimSpace(line[3:len(line)])
		} else if strings.HasPrefix(line, "env:") {
			env = strings.TrimSpace(line[4:len(line)])
		}
	}
	if az != "" {
		colo = az
	} else if dc != "" {
		colo = dc
	}
	return colo, env, err
}

func getResourceInfo(appName string) *resource.Resource {
	colo, env, _err := getEnvFromSyshieraYaml()
	if _err != nil {
		logger.GetLogger().Log(logger.Alert, "Error while reading /etc/syshiera.yaml file. ", _err.Error())
	}
	hostname, _ := os.Hostname()

	resource := resource.NewWithAttributes("juno resource",
		attribute.String("container_host", hostname),
		attribute.String("az", colo),
		attribute.String("environment", env),
		attribute.String("application", appName),
	)
	return resource
}
