package otel

import (
	"context"
	"fmt"
	"github.com/paypal/hera/utility/logger"
	otelconfig "github.com/paypal/hera/utility/logger/otel/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const defaultAppName string = "occ"

// Implement apply function in to configure meter provider
func (o MetricProviderOption) apply(c *stateLogMetricsConfig) {
	if o.MeterProvider != nil {
		c.MeterProvider = o.MeterProvider
	}
}

// Implement apply function in to configure pool name
func (appName AppNameOption) apply(c *stateLogMetricsConfig) {
	if appName != "" {
		c.appName = string(appName)
	}
}

// WithAppName Create StateLogMetrics with OCC Name
func WithAppName(appName string) StateLogOption {
	return AppNameOption(appName)
}

// WithMetricProvider Create StateLogMetrics with provided meter Provider
func WithMetricProvider(provider metric.MeterProvider) StateLogOption {
	return MetricProviderOption{provider}
}

// newConfig computes a config from the supplied Options.
func newConfig(opts ...StateLogOption) stateLogMetricsConfig {
	statesConfig := stateLogMetricsConfig{
		MeterProvider: otel.GetMeterProvider(),
		appName:       defaultAppName,
	}

	for _, opt := range opts {
		opt.apply(&statesConfig)
	}
	return statesConfig
}

// StartMetricsCollection initializes reporting of stateLogMetrics using the supplied config.
func StartMetricsCollection(stateLogDataChan <-chan WorkersStateData, opt ...StateLogOption) error {
	stateLogMetricsConfig := newConfig(opt...)

	//Verification of config data
	if stateLogMetricsConfig.appName == "" {
		stateLogMetricsConfig.appName = defaultAppName
	}

	if stateLogMetricsConfig.MeterProvider == nil {
		stateLogMetricsConfig.MeterProvider = otel.GetMeterProvider()
	}

	//Initialize state-log metrics
	stateLogMetrics := &StateLogMetrics{
		meter: stateLogMetricsConfig.MeterProvider.Meter(StateLogMeterName,
			metric.WithInstrumentationVersion(OtelInstrumentationVersion)),
		metricsConfig:  stateLogMetricsConfig,
		mStateDataChan: stateLogDataChan,
		doneCh:         make(chan struct{}),
	}

	var err error
	//Registers instrumentation for metrics
	stateLogMetrics.registerStateMetrics.Do(func() {
		err = stateLogMetrics.register()
	})
	return err
}

// Define Instrumentation for each metrics and register with StateLogMetrics
func (stateLogMetrics *StateLogMetrics) register() error {

	//"init", "acpt", "wait", "busy", "schd", "fnsh", "quce", "asgn", "idle", "bklg", "strd", "cls"
	var err error
	if stateLogMetrics.initState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(InitConnCountMetric),
		metric.WithDescription("Number of workers in init state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init state", err)
		return err
	}

	if stateLogMetrics.acptState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AccptConnCountMetric),
		metric.WithDescription("Number of workers in accept state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for accept state", err)
		return err
	}

	if stateLogMetrics.waitState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WaitConnCountMetric),
		metric.WithDescription("Number of workers in wait state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for wait state", err)
		return err
	}

	if stateLogMetrics.busyState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BusyConnCountMetric),
		metric.WithDescription("Number of workers in busy state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for busy state", err)
		return err
	}

	if stateLogMetrics.schdState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(ScheduledConnCountMetric),
		metric.WithDescription("Number of workers in scheduled state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state", err)
		return err
	}

	if stateLogMetrics.fnshState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(FinishedConnCountMetric),
		metric.WithDescription("Number of workers in finished state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for finished state", err)
		return err
	}

	if stateLogMetrics.quceState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(QuiescedConnCountMetric),
		metric.WithDescription("Number of workers in quiesced state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state", err)
		return err
	}

	if stateLogMetrics.asgnState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AssignedConnCountMetric),
		metric.WithDescription("Number of workers in assigned state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for assigned state", err)
		return err
	}

	if stateLogMetrics.idleState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(IdleConnCountMetric),
		metric.WithDescription("Number of workers in idle state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for idle state", err)
		return err
	}

	if stateLogMetrics.bklgState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BacklogConnCountMetric),
		metric.WithDescription("Number of workers in backlog state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for backlog state", err)
		return err
	}

	if stateLogMetrics.strdState, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(StrdConnCountMetric),
		metric.WithDescription("Number of connections in stranded state"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for stranded state", err)
		return err
	}

	if stateLogMetrics.initStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(InitConnCountMetricMax),
		metric.WithDescription("Number of workers in init state max count value"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for init state max count", err)
		return err
	}

	if stateLogMetrics.acptStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AccptConnCountMetricMax),
		metric.WithDescription("Number of workers in accept state count max"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for accept state max count", err)
		return err
	}

	if stateLogMetrics.waitStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WaitConnCountMetricMax),
		metric.WithDescription("Number of workers in wait state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for wait state max count", err)
		return err
	}

	if stateLogMetrics.busyStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BusyConnCountMetricMax),
		metric.WithDescription("Number of workers in busy state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for busy state max count", err)
		return err
	}

	if stateLogMetrics.schdStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(ScheduledConnCountMetricMax),
		metric.WithDescription("Number of workers in scheduled state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for scheduled state max count", err)
		return err
	}

	if stateLogMetrics.fnshStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(FinishedConnCountMetricMax),
		metric.WithDescription("Number of workers in finished state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for finished state max count", err)
		return err
	}

	if stateLogMetrics.quceStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(QuiescedConnCountMetricMax),
		metric.WithDescription("Number of workers in quiesced state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for quiesced state max count", err)
		return err
	}

	if stateLogMetrics.asgnStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(AssignedConnCountMetricMax),
		metric.WithDescription("Number of workers in assigned state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for assigned state max count", err)
		return err
	}

	if stateLogMetrics.idleStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(IdleConnCountMetricMax),
		metric.WithDescription("Number of workers in idle state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for idle state max count", err)
		return err
	}

	if stateLogMetrics.bklgStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(BacklogConnCountMetricMax),
		metric.WithDescription("Number of workers in backlog state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for backlog state max count", err)
		return err
	}

	if stateLogMetrics.strdStateMax, err = stateLogMetrics.meter.Int64ObservableGauge(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(StrdConnCountMetricMax),
		metric.WithDescription("Number of connections in stranded state max count"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register guage metric for stranded state max count", err)
		return err
	}

	if stateLogMetrics.workerReqCount, err = stateLogMetrics.meter.Int64ObservableUpDownCounter(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WorkerRequestCountMetric),
		metric.WithDescription("Number requests handled by worker between pooling period"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register updown counter metric for request handled by worker", err)
		return err
	}

	if stateLogMetrics.workerRespCount, err = stateLogMetrics.meter.Int64ObservableUpDownCounter(
		otelconfig.OTelConfigData.PopulateMetricNamePrefix(WorkerResponseCountMetric),
		metric.WithDescription("Number responses served by worker between pooling period"),
	); err != nil {
		logger.GetLogger().Log(logger.Alert, "Failed to register updown counter metric for responses served by worker", err)
		return err
	}

	stateLogMetrics.registration, err = stateLogMetrics.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			return stateLogMetrics.asyncStatelogMetricsPoll(ctx, observer)
		},
		[]metric.Observable{
			stateLogMetrics.initState,
			stateLogMetrics.acptState,
			stateLogMetrics.waitState,
			stateLogMetrics.busyState,
			stateLogMetrics.schdState,
			stateLogMetrics.fnshState,
			stateLogMetrics.quceState,
			stateLogMetrics.asgnState,
			stateLogMetrics.idleState,
			stateLogMetrics.bklgState,
			stateLogMetrics.strdState,
			stateLogMetrics.initStateMax,
			stateLogMetrics.acptStateMax,
			stateLogMetrics.waitStateMax,
			stateLogMetrics.busyStateMax,
			stateLogMetrics.schdStateMax,
			stateLogMetrics.fnshStateMax,
			stateLogMetrics.quceStateMax,
			stateLogMetrics.asgnStateMax,
			stateLogMetrics.idleStateMax,
			stateLogMetrics.bklgStateMax,
			stateLogMetrics.strdStateMax,
			stateLogMetrics.workerReqCount,
			stateLogMetrics.workerRespCount,
		}...)

	if err != nil {
		return err
	}
	return nil
}

/*
 * AasyncStatelogMetricsPoll poll operation involved periodically by OTEL collector based-on its polling interval
 * it poll metrics from channel do aggregation or compute max based combination of shardId + workerType + InstanceId
 */
func (stateLogMetrics *StateLogMetrics) asyncStatelogMetricsPoll(ctx context.Context, observer metric.Observer) (err error) {
	stateLogMetrics.recordLock.Lock()
	defer stateLogMetrics.recordLock.Unlock()
	stateLogsData := make(map[string]map[string]int64)
	//Infinite loop read through the channel and send metrics
mainloop:
	for {
		select {
		case workersState, more := <-stateLogMetrics.mStateDataChan:
			if !more { // TODO:: check zero value for workersState
				logger.GetLogger().Log(logger.Info, "Statelog metrics data channel 'mStateDataChan' has been closed.")
				break mainloop
			}
			keyName := fmt.Sprintf("%d-%d-%d", workersState.ShardId, workersState.WorkerType, workersState.InstanceId)

			if stateLogsData[keyName] == nil {
				stateLogsData[keyName] = make(map[string]int64)
			}
			//Update metadata information
			stateLogsData[keyName][ShardId] = int64(workersState.ShardId)
			stateLogsData[keyName][WorkerType] = int64(workersState.WorkerType)
			stateLogsData[keyName][InstanceId] = int64(workersState.InstanceId)
			stateLogsData[keyName][Datapoints] += 1

			for key, value := range workersState.StateData {
				if key == "req" || key == "resp" {
					stateLogsData[keyName][key] += value
				} else {
					maxKey := key + "Max"
					stateLogsData[keyName][key] = value
					//check max update max value
					if stateLogsData[keyName][maxKey] < value {
						stateLogsData[keyName][maxKey] = value
					}
				}

			}
		case <-stateLogMetrics.doneCh:
			logger.GetLogger().Log(logger.Info, "received stopped signal for processing statelog metric. so unregistering callback for sending data")
			stateLogMetrics.registration.Unregister()
		default:
			break mainloop
		}
	}
	//Process metrics data
	err = stateLogMetrics.sendMetricsDataToCollector(ctx, observer, stateLogsData)
	return err
}

/*
 *  Send metrics datat data-points to collector
 */
func (stateLogMetrics *StateLogMetrics) sendMetricsDataToCollector(ctx context.Context, observer metric.Observer, stateLogsData map[string]map[string]int64) (err error) {
	for key, aggStatesData := range stateLogsData {
		logger.GetLogger().Log(logger.Info, fmt.Sprintf("calculated max value and aggregation of updown counter for key: %s using datapoints size: %d", key, aggStatesData[Datapoints]))
		commonLabels := []attribute.KeyValue{
			attribute.String(Application, stateLogMetrics.metricsConfig.appName),
			attribute.Int(ShardId, int(aggStatesData[ShardId])),
			attribute.Int(WorkerType, int(aggStatesData[WorkerType])),
			attribute.Int(InstanceId, int(aggStatesData[InstanceId])),
		}

		//Observe states data
		// 1. Worker States
		observer.ObserveInt64(stateLogMetrics.initState, aggStatesData["init"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.acptState, aggStatesData["acpt"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.waitState, aggStatesData["wait"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.busyState, aggStatesData["busy"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.schdState, aggStatesData["schd"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.fnshState, aggStatesData["fnsh"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.quceState, aggStatesData["quce"], metric.WithAttributes(commonLabels...))

		// 2. Connection States
		observer.ObserveInt64(stateLogMetrics.asgnState, aggStatesData["asgn"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.idleState, aggStatesData["idle"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.bklgState, aggStatesData["bklg"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.strdState, aggStatesData["strd"], metric.WithAttributes(commonLabels...))

		//3. Max Worker States
		observer.ObserveInt64(stateLogMetrics.initStateMax, aggStatesData["initMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.acptStateMax, aggStatesData["acptMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.waitStateMax, aggStatesData["waitMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.busyStateMax, aggStatesData["busyMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.schdStateMax, aggStatesData["schdMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.fnshStateMax, aggStatesData["fnshMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.quceStateMax, aggStatesData["quceMax"], metric.WithAttributes(commonLabels...))

		// 4. Max Connection States
		observer.ObserveInt64(stateLogMetrics.asgnStateMax, aggStatesData["asgnMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.idleStateMax, aggStatesData["idleMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.bklgStateMax, aggStatesData["bklgMax"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.strdStateMax, aggStatesData["strdMax"], metric.WithAttributes(commonLabels...))

		//Workers stats
		observer.ObserveInt64(stateLogMetrics.workerReqCount, aggStatesData["req"], metric.WithAttributes(commonLabels...))
		observer.ObserveInt64(stateLogMetrics.workerRespCount, aggStatesData["resp"], metric.WithAttributes(commonLabels...))
	}
	return nil
}
