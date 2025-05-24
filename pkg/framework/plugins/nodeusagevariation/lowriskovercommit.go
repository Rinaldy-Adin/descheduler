package nodeusagevariation

import (
	"context"
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
)

const LowRiskOvercommitPluginName = "LowRiskOvercommit"

var _ frameworktypes.BalancePlugin = &LowRiskOvercommit{}

type LowRiskOvercommit struct {
	handle            frameworktypes.Handle
	args              *LowRiskOvercommitArgs
	resourceNames     []v1.ResourceName
	podFilter         func(pod *v1.Pod) bool
	avgUsageClient    usageClient
	stdDevUsageClient usageClient
}

func NewLowRiskOvercommit(
	genericArgs runtime.Object, handle frameworktypes.Handle,
) (frameworktypes.Plugin, error) {
	args, ok := genericArgs.(*LowRiskOvercommitArgs)
	if !ok {
		return nil, fmt.Errorf(
			"want args to be of type LowRiskOvercommitArgs, got %T",
			genericArgs,
		)
	}

	klog.V(1).Info("using LowRiskOvercommit")

	podFilter, err := podutil.
		NewOptions().
		WithFilter(handle.Evictor().Filter).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	resourceNames := []v1.ResourceName{v1.ResourceCPU}

	if handle.PrometheusClient() == nil {
		return nil, fmt.Errorf("prometheus client not initialized")
	}

	avgUsageClient := newPrometheusUsageClient(
		handle.GetPodsAssignedToNodeFunc(),
		handle.PrometheusClient(),
		// TODO: make sure query is between 0 and 1
		`
		label_replace(
		  (
			1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1m]))
		  )
			* on(instance) group_left(nodename)
			node_uname_info,
		  "instance", "$1", "nodename", "(.*)"
		)
		`,
		`sum by (pod) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))`,
	)

	stdDevUsageClient := newPrometheusUsageClient(
		handle.GetPodsAssignedToNodeFunc(),
		handle.PrometheusClient(),
		//  TODO: make sure this is correct with query for average
		`
			label_replace(
			  (
				stddev_over_time( sum by (instance) ( rate(node_cpu_seconds_total{mode!="idle"}[1m]))[5m:])
			  )
				* on(instance) group_left(nodename)
				node_uname_info,
			  "instance", "$1", "nodename", "(.*)"
			)
		`,
		`stddev_over_time( sum by (pod) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))[5m:])`,
	)

	return &LowRiskOvercommit{
		handle:            handle,
		args:              args,
		resourceNames:     resourceNames,
		podFilter:         podFilter,
		avgUsageClient:    avgUsageClient,
		stdDevUsageClient: stdDevUsageClient,
	}, nil
}

func (l *LowRiskOvercommit) Name() string {
	return LowRiskOvercommitPluginName
}

func (l *LowRiskOvercommit) Balance(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	if err := l.avgUsageClient.sync(ctx, nodes); err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error getting average node usage: %v", err),
		}
	}

	if err := l.stdDevUsageClient.sync(ctx, nodes); err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error getting average node usage: %v", err),
		}
	}

	nodesMap, rawAvgUsage, rawStdDevUsage, podListMap := getNodeUsageDistributionSnapshot(nodes, l.avgUsageClient, l.stdDevUsageClient)
	capacities := getCPUNodeCapacities(nodes)

	rawAvgUsageLogKeys := quantityMapsToKeysAndValues(rawAvgUsage)
	klog.V(1).InfoS(
		"Raw Average Usage Snapshot",
		rawAvgUsageLogKeys...,
	)

	rawStdDevUsageLogKeys := quantityMapsToKeysAndValues(rawAvgUsage)
	klog.V(1).InfoS(
		"Raw Standard Deviation Usage Snapshot",
		rawStdDevUsageLogKeys...,
	)

	usageMap := rawUsageToPctUsageMap(rawAvgUsage, rawStdDevUsage, capacities)

	underUsedNodes, overUsedNodes := l.classifyLoadDistribution(nodesMap, usageMap, podListMap, capacities)

	if len(overUsedNodes) == 0 {
		klog.V(1).InfoS(
			"No node is high risk, nothing to do here",
		)
		return nil
	}

	if len(underUsedNodes) == 0 {
		klog.V(1).InfoS("All nodes are high risk of overcommitting, nothing the descheduler can do here, try to add more nodes")
		return nil
	}

	l.sortNodesByUsageRisk(overUsedNodes, false)

	// this is a stop condition for the eviction process. we stop as soon
	// as the node usage drops below the threshold.
	continueEvictionCond := func(nodeInfo NodeDistributionInfo, totalAvailableUsage resource.Quantity) bool {
		if !l.isNodeAboveTargetRisk(nodeInfo) {
			return false
		}

		if totalAvailableUsage.CmpInt64(0) < 1 {
			return false
		}

		return true
	}

	var nodeLimit *uint
	if l.args.EvictionLimits != nil {
		nodeLimit = l.args.EvictionLimits.Node
	}

	sortPodsByRisk := func(
		node NodeDistributionInfo,
		pods []*v1.Pod,
		avgUsageClient, stdDevUsageClient usageClient,
	) {
		podAvgUsage := make(map[string]*resource.Quantity)
		podStdDevUsage := make(map[string]*resource.Quantity)

		for _, pod := range pods {
			avg, _ := avgUsageClient.podUsage(pod)
			stdDev, _ := stdDevUsageClient.podUsage(pod)

			podAvgUsage[pod.Name] = avg[MetricResource]
			podStdDevUsage[pod.Name] = stdDev[MetricResource]
		}

		sort.Slice(pods, func(i, j int) bool {
			pi := podAvgUsage[pods[i].Name].MilliValue() + podStdDevUsage[pods[i].Name].MilliValue()
			pj := podAvgUsage[pods[j].Name].MilliValue() + podStdDevUsage[pods[j].Name].MilliValue()

			return pi < pj
		})
	}

	evictPodsFromSourceNodes(
		ctx,
		l.args.EvictableNamespaces,
		overUsedNodes,
		underUsedNodes,
		l.handle.Evictor(),
		evictions.EvictOptions{StrategyName: LowRiskOvercommitPluginName},
		l.podFilter,
		l.resourceNames,
		continueEvictionCond,
		l.avgUsageClient,
		l.stdDevUsageClient,
		sortPodsByRisk,
		nodeLimit,
	)

	return nil
}

func (l *LowRiskOvercommit) classifyLoadDistribution(
	nodeMap map[string]*v1.Node,
	usageMap map[string]ResourceUsageDistributions,
	podListMap map[string][]*v1.Pod,
	capacityMap map[string]resource.Quantity,
) (
	[]NodeDistributionInfo,
	[]NodeDistributionInfo,
) {

	var (
		underUsedNodes = make([]NodeDistributionInfo, 0)
		overUsedNodes  = make([]NodeDistributionInfo, 0)

		totalRequestsMap = make(map[string]*resource.Quantity)
		totalLimitsMap   = make(map[string]*resource.Quantity)

		clusterTotalLimit    = resource.NewQuantity(0, resource.BinarySI)
		clusterTotalCapacity = resource.NewQuantity(0, resource.BinarySI)
	)

	for nodeName, podList := range podListMap {
		nodeTotalRequests := resource.NewQuantity(0, resource.BinarySI)
		nodeTotalLimits := resource.NewQuantity(0, resource.BinarySI)

		for _, pod := range podList {
			var podRequest, podLimit resource.Quantity
			//if pod.Spec.Resources == nil {

			//}
			//podRequest := pod.Spec.Resources.Requests[v1.ResourceCPU]
		}
	}

	for nodeName := range nodeMap {
		mu := usageMap[nodeName].avg
		sigma := usageMap[nodeName].stdDev

		risk := mu + sigma

		// TODO: threshold args
		var sliceToAppend *[]NodeDistributionInfo
		if risk > 100 {
			sliceToAppend = &overUsedNodes
			klog.V(1).InfoS("Node classified as high risk", "node", klog.KObj(nodeMap[nodeName]), "mu", mu, "sigma", sigma, "risk", risk)
		} else {
			sliceToAppend = &underUsedNodes
			klog.V(1).InfoS("Node classified as low risk", "node", klog.KObj(nodeMap[nodeName]), "mu", mu, "sigma", sigma, "risk", risk)
		}

		*sliceToAppend = append(*sliceToAppend, NodeDistributionInfo{
			NodeDistributionUsage: NodeDistributionUsage{
				node:     nodeMap[nodeName],
				avg:      PercentageToResourceQuantity(mu, capacityMap[nodeName]),
				stdDev:   PercentageToResourceQuantity(sigma, capacityMap[nodeName]),
				capacity: capacityMap[nodeName],
				allPods:  podListMap[nodeName],
			},
			available: capacityMap[nodeName],
		})
	}

	return underUsedNodes, overUsedNodes
}

func (l *LowRiskOvercommit) calculateRiskFromQuantities(avg, stdDev, capacity resource.Quantity) api.Percentage {
	return api.Percentage(float64(avg.MilliValue()+stdDev.MilliValue()) / float64(capacity.MilliValue()))
}

func (l *LowRiskOvercommit) sortNodesByUsageRisk(
	nodes []NodeDistributionInfo,
	ascending bool,
) {
	sort.Slice(nodes, func(i, j int) bool {
		ti := l.calculateRiskFromQuantities(nodes[i].avg, nodes[i].stdDev, nodes[i].capacity)
		tj := l.calculateRiskFromQuantities(nodes[j].avg, nodes[j].stdDev, nodes[j].capacity)

		if ascending {
			return ti < tj
		}

		return ti > tj
	})
}

func (l *LowRiskOvercommit) isNodeAboveTargetRisk(nodeInfo NodeDistributionInfo) bool {
	risk := l.calculateRiskFromQuantities(nodeInfo.avg, nodeInfo.stdDev, nodeInfo.capacity)

	// TODO: use ita for threshold
	return risk > 100.
}
