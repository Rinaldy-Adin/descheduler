package nodeusagevariation

import (
	"context"
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/normalizer"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
	"sigs.k8s.io/descheduler/pkg/utils"
)

const LoadVariationRiskBalancingPluginName = "LodVariationRiskBalancing"

type continueEvictionCond func(NodeDistributionInfo, resource.Quantity) bool

var _ frameworktypes.BalancePlugin = &LoadVariationRiskBalancing{}

type LoadVariationRiskBalancing struct {
	handle            frameworktypes.Handle
	args              *LoadVariationRiskBalancingArgs
	resourceNames     []v1.ResourceName
	podFilter         func(pod *v1.Pod) bool
	avgUsageClient    usageClient
	stdDevUsageClient usageClient
}

type ResourceUsageDistributions struct {
	avg    api.Percentage
	stdDev api.Percentage
}

type NodeDistributionUsage struct {
	node     *v1.Node
	avg      resource.Quantity
	stdDev   resource.Quantity
	capacity resource.Quantity
	allPods  []*v1.Pod
}

// NodeInfo is an entity we use to gather information about a given node. here
// we have its resource usage as well as the amount of available resources.
// we use this struct to carry information around and to make it easier to
// process.
type NodeDistributionInfo struct {
	NodeDistributionUsage
	available resource.Quantity
}

func NewLoadVariationRiskBalancing(
	genericArgs runtime.Object, handle frameworktypes.Handle,
) (frameworktypes.Plugin, error) {
	args, ok := genericArgs.(*LoadVariationRiskBalancingArgs)
	if !ok {
		return nil, fmt.Errorf(
			"want args to be of type LoadVariationRiskBalancingArgs, got %T",
			genericArgs,
		)
	}

	// if we are using prometheus we need to validate we have everything we
	// need. if we aren't then we need to make sure we are also collecting
	// data for cpu, memory and pods.
	metrics := args.MetricsUtilization

	if metrics == nil {
		return nil, fmt.Errorf("metrics args are missing")
	}

	if args.MetricsUtilization.Prometheus == nil {
		return nil, fmt.Errorf("prometheus property is missing")
	}

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
		`(sum by (instance) (rate(node_cpu_seconds_total{mode!="idle"}[1m]))) / (count by (instance) (node_cpu_seconds_total{mode="idle"}))`,
		`sum by (pod) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))`,
	)

	stdDevUsageClient := newPrometheusUsageClient(
		handle.GetPodsAssignedToNodeFunc(),
		handle.PrometheusClient(),
		//  TODO: make sure this is correct with query for average
		`stddev_over_time( sum by (instance) ( rate(node_cpu_seconds_total{mode!="idle"}[1m]))[5m:])`,
		`stddev_over_time( sum by (pod) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))[5m:])`,
	)

	return &LoadVariationRiskBalancing{
		handle:            handle,
		args:              args,
		resourceNames:     resourceNames,
		podFilter:         podFilter,
		avgUsageClient:    avgUsageClient,
		stdDevUsageClient: stdDevUsageClient,
	}, nil
}

func (l *LoadVariationRiskBalancing) Name() string {
	return LoadVariationRiskBalancingPluginName
}

func (l *LoadVariationRiskBalancing) Balance(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
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

	// TODO: check usage map is already divided by capacity or not, make sure usage map has raw Data
	nodesMap, rawAvgUsage, rawStdDevUsage, podListMap := getNodeUsageDistributionSnapshot(nodes, l.avgUsageClient, l.stdDevUsageClient)
	capacities := getCPUNodeCapacities(nodes)

	usageMap := rawUsageToPctUsageMap(rawAvgUsage, rawStdDevUsage, capacities)

	// TODO: logging
	lowRiskNodes, highRiskNodes := l.classifyLoadDistribution(nodesMap, usageMap, podListMap, capacities)

	if len(highRiskNodes) == 0 {
		klog.V(1).InfoS(
			"No node is high risk, nothing to do here",
		)
		return nil
	}

	if len(lowRiskNodes) == 0 {
		klog.V(1).InfoS("All nodes are high risk of overcommitting, nothing the descheduler can do here, try to add more nodes")
		return nil
	}

	sortNodesByUsageRisk(highRiskNodes, false)

	// this is a stop condition for the eviction process. we stop as soon
	// as the node usage drops below the threshold.
	continueEvictionCond := func(nodeInfo NodeDistributionInfo, totalAvailableUsage resource.Quantity) bool {
		if !isNodeAboveTargetRisk(nodeInfo) {
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

	evictPodsFromSourceNodes(
		ctx,
		l.args.EvictableNamespaces,
		highRiskNodes,
		lowRiskNodes,
		l.handle.Evictor(),
		evictions.EvictOptions{StrategyName: LoadVariationRiskBalancingPluginName},
		l.podFilter,
		l.resourceNames,
		continueEvictionCond,
		l.avgUsageClient,
		l.stdDevUsageClient,
		nodeLimit,
	)

	return nil
}

func evictPodsFromSourceNodes(
	ctx context.Context,
	evictableNamespaces *api.Namespaces,
	sourceNodes, destinationNodes []NodeDistributionInfo,
	podEvictor frameworktypes.Evictor,
	evictOptions evictions.EvictOptions,
	podFilter func(pod *v1.Pod) bool,
	resourceNames []v1.ResourceName,
	continueEviction continueEvictionCond,
	avgUsageClient, stdDevUsageClient usageClient,
	maxNoOfPodsToEvictPerNode *uint,
) {
	available := assessAvailableResourceInNodes(destinationNodes)
	klog.V(1).InfoS("Total capacity to be moved", usageToKeysAndValues(available)...)

	destinationTaints := make(map[string][]v1.Taint, len(destinationNodes))
	for _, node := range destinationNodes {
		destinationTaints[node.node.Name] = node.node.Spec.Taints
	}

	for _, node := range sourceNodes {
		klog.V(3).InfoS(
			"Evicting pods from node",
			"node", klog.KObj(node.node),
			"avg", node.avg,
		)

		nonRemovablePods, removablePods := classifyPods(node.allPods, podFilter)
		klog.V(2).InfoS(
			"Pods on node",
			"node", klog.KObj(node.node),
			"allPods", len(node.allPods),
			"nonRemovablePods", len(nonRemovablePods),
			"removablePods", len(removablePods),
		)

		if len(removablePods) == 0 {
			klog.V(1).InfoS(
				"No removable pods on node, try next node",
				"node", klog.KObj(node.node),
			)
			continue
		}

		klog.V(1).InfoS(
			"Evicting pods based on priority, if they have same priority, they'll be evicted based on QoS tiers",
		)

		sortPodsByRisk(removablePods, avgUsageClient, stdDevUsageClient)

		if err := evictPods(
			ctx,
			evictableNamespaces,
			removablePods,
			node,
			available,
			destinationTaints,
			podEvictor,
			evictOptions,
			continueEviction,
			avgUsageClient,
			maxNoOfPodsToEvictPerNode,
		); err != nil {
			switch err.(type) {
			case *evictions.EvictionTotalLimitError:
				return
			default:
			}
		}
	}
}

func evictPods(
	ctx context.Context,
	evictableNamespaces *api.Namespaces,
	inputPods []*v1.Pod,
	nodeInfo NodeDistributionInfo,
	totalAvailableUsage resource.Quantity,
	destinationTaints map[string][]v1.Taint,
	podEvictor frameworktypes.Evictor,
	evictOptions evictions.EvictOptions,
	continueEviction continueEvictionCond,
	usageClient usageClient,
	maxNoOfPodsToEvictPerNode *uint,
) error {
	// preemptive check to see if we should continue evicting pods.
	if !continueEviction(nodeInfo, totalAvailableUsage) {
		return nil
	}

	// some namespaces can be excluded from the eviction process.
	var excludedNamespaces sets.Set[string]
	if evictableNamespaces != nil {
		excludedNamespaces = sets.New(evictableNamespaces.Exclude...)
	}

	var evictionCounter uint = 0
	for _, pod := range inputPods {
		if maxNoOfPodsToEvictPerNode != nil && evictionCounter >= *maxNoOfPodsToEvictPerNode {
			klog.V(3).InfoS(
				"Max number of evictions per node per plugin reached",
				"limit", *maxNoOfPodsToEvictPerNode,
			)
			break
		}

		if !utils.PodToleratesTaints(pod, destinationTaints) {
			klog.V(3).InfoS(
				"Skipping eviction for pod, doesn't tolerate node taint",
				"pod", klog.KObj(pod),
			)
			continue
		}

		// verify if we can evict the pod based on the pod evictor
		// filter and on the excluded namespaces.
		preEvictionFilterWithOptions, err := podutil.
			NewOptions().
			WithFilter(podEvictor.PreEvictionFilter).
			WithoutNamespaces(excludedNamespaces).
			BuildFilterFunc()
		if err != nil {
			klog.ErrorS(err, "could not build preEvictionFilter with namespace exclusion")
			continue
		}

		if !preEvictionFilterWithOptions(pod) {
			continue
		}

		// in case podUsage does not support resource counting (e.g.
		// provided metric does not quantify pod resource utilization).
		unconstrainedResourceEviction := false
		podUsageResourceList, err := usageClient.podUsage(pod)
		if err != nil {
			if _, ok := err.(*notSupportedError); !ok {
				klog.Errorf(
					"unable to get pod usage for %v/%v: %v",
					pod.Namespace, pod.Name, err,
				)
				continue
			}
			unconstrainedResourceEviction = true
		}

		podUsage := podUsageResourceList[MetricResource]

		if err := podEvictor.Evict(ctx, pod, evictOptions); err != nil {
			switch err.(type) {
			case *evictions.EvictionNodeLimitError, *evictions.EvictionTotalLimitError:
				return err
			default:
				klog.Errorf("eviction failed: %v", err)
				continue
			}
		}

		if maxNoOfPodsToEvictPerNode == nil && unconstrainedResourceEviction {
			klog.V(3).InfoS("Currently, only a single pod eviction is allowed")
			break
		}

		evictionCounter++
		klog.V(3).InfoS("Evicted pods", "pod", klog.KObj(pod))
		if unconstrainedResourceEviction {
			continue
		}

		subtractPodUsageFromNodeAvailability(&totalAvailableUsage, &nodeInfo, podUsage)

		keysAndValues := []any{"node", nodeInfo.node.Name}
		keysAndValues = append(keysAndValues, usageToKeysAndValues(nodeInfo.avg)...)
		klog.V(3).InfoS("Updated node usage", keysAndValues...)

		// make sure we should continue evicting pods.
		if !continueEviction(nodeInfo, totalAvailableUsage) {
			break
		}
	}
	return nil
}

func subtractPodUsageFromNodeAvailability(
	available *resource.Quantity,
	nodeInfo *NodeDistributionInfo,
	podUsage *resource.Quantity,
) {
	nodeInfo.avg.Sub(*podUsage)
	available.Sub(*podUsage)
}

func assessAvailableResourceInNodes(
	nodes []NodeDistributionInfo,
) resource.Quantity {
	available := resource.NewQuantity(0, resource.BinarySI)
	for _, node := range nodes {
		usage := node.avg

		available.Add(node.available)
		available.Sub(usage)
	}

	return *available
}

func rawUsageToPctUsageMap(
	rawAvgUsage, rawStdDevUsage, rawCapacities map[string]resource.Quantity,
) map[string]ResourceUsageDistributions {
	avgUsage := normalizer.Normalize(
		rawAvgUsage, rawCapacities, ResourceQuantityToPercentage,
	)

	stdDevUsage := normalizer.Normalize(
		rawStdDevUsage, rawCapacities, ResourceQuantityToPercentage,
	)

	usageMap := make(map[string]ResourceUsageDistributions)
	for nodeName := range rawAvgUsage {
		usageMap[nodeName] = ResourceUsageDistributions{
			avg:    avgUsage[nodeName],
			stdDev: stdDevUsage[nodeName],
		}
	}

	return usageMap
}

func getNodeUsageDistributionSnapshot(
	nodes []*v1.Node,
	avgUsageClient usageClient,
	stdDevUsageClient usageClient,
) (
	map[string]*v1.Node,
	map[string]resource.Quantity,
	map[string]resource.Quantity,
	map[string][]*v1.Pod,
) {
	rawAvgUsage := make(map[string]resource.Quantity)
	rawStdDevUsage := make(map[string]resource.Quantity)
	podListMap := make(map[string][]*v1.Pod)
	nodesMap := make(map[string]*v1.Node)

	for _, node := range nodes {
		nodesMap[node.Name] = node
		rawAvgUsage[node.Name] = *avgUsageClient.nodeUtilization(node.Name)[MetricResource]
		rawStdDevUsage[node.Name] = *stdDevUsageClient.nodeUtilization(node.Name)[MetricResource]
		podListMap[node.Name] = avgUsageClient.pods(node.Name)
	}

	return nodesMap, rawAvgUsage, rawStdDevUsage, podListMap
}

// TODO: implement
func (l *LoadVariationRiskBalancing) classifyLoadDistribution(
	nodeMap map[string]*v1.Node,
	usageMap map[string]ResourceUsageDistributions,
	podListMap map[string][]*v1.Pod,
	capacityMap map[string]resource.Quantity,
) (
	[]NodeDistributionInfo,
	[]NodeDistributionInfo,
) {
	lowRiskNodes := make([]NodeDistributionInfo, 0)
	highRiskNodes := make([]NodeDistributionInfo, 0)

	for nodeName := range nodeMap {
		mu := usageMap[nodeName].avg
		sigma := usageMap[nodeName].stdDev

		// TODO: add args for sensitivity & margin, are 1 by default anyway

		// apply root power
		//if sensitivity >= 0 {
		//sigma = math.Pow(sigma, 1/sensitivity)
		//}

		// apply multiplier
		//sigma *= margin
		//sigma = max(min(sigma, 1), 0)

		// evaluate overall risk factor
		risk := mu + sigma
		klog.V(6).Info("Evaluating risk factor", "mu", mu, "sigma", sigma, "risk", risk)

		// TODO: threshold args
		var sliceToAppend *[]NodeDistributionInfo
		if risk > 1 {
			sliceToAppend = &highRiskNodes
		} else {
			sliceToAppend = &lowRiskNodes
		}

		*sliceToAppend = append(*sliceToAppend, NodeDistributionInfo{
			NodeDistributionUsage: NodeDistributionUsage{
				node:     nodeMap[nodeName],
				avg:      PercentageToResourceQuantity(mu, capacityMap[nodeName]),
				stdDev:   PercentageToResourceQuantity(sigma, capacityMap[nodeName]),
				capacity: capacityMap[nodeName],
				allPods:  podListMap[nodeName],
			},
		})
	}

	return lowRiskNodes, highRiskNodes
}

func calculateRiskFromQuantities(avg, stdDev, capacity resource.Quantity) api.Percentage {
	return api.Percentage(float64(avg.MilliValue()+stdDev.MilliValue()) / float64(capacity.MilliValue()))
}

// TODO: weight between avg and vairance
func sortNodesByUsageRisk(
	nodes []NodeDistributionInfo,
	ascending bool,
) {
	sort.Slice(nodes, func(i, j int) bool {
		ti := calculateRiskFromQuantities(nodes[i].avg, nodes[i].stdDev, nodes[i].capacity)
		tj := calculateRiskFromQuantities(nodes[j].avg, nodes[j].stdDev, nodes[j].capacity)

		if ascending {
			return ti < tj
		}

		return ti > tj
	})
}

func getCPUNodeCapacities(nodes []*v1.Node) map[string]resource.Quantity {
	capacities := map[string]resource.Quantity{}
	for _, node := range nodes {
		capacities[node.Name] = *node.Status.Allocatable.Cpu()
	}
	return capacities
}

func ResourceQuantityToPercentage(
	value, total resource.Quantity,
) api.Percentage {
	return api.Percentage(value.MilliValue() / total.MilliValue())
}

func PercentageToResourceQuantity(
	percentage api.Percentage,
	total resource.Quantity,
) resource.Quantity {
	return *resource.NewMilliQuantity(
		int64(
			float64(total.MilliValue())*float64(percentage)*0.01,
		), resource.BinarySI)
}

func isNodeAboveTargetRisk(nodeInfo NodeDistributionInfo) bool {
	risk := calculateRiskFromQuantities(nodeInfo.avg, nodeInfo.stdDev, nodeInfo.capacity)

	// TODO: use ita for threshold
	return risk > 100.
}

func usageToKeysAndValues(usage resource.Quantity) []any {
	keysAndValues := []any{}
	keysAndValues = append(keysAndValues, "MetricResource", usage.MilliValue())
	return keysAndValues
}

func classifyPods(pods []*v1.Pod, filter func(pod *v1.Pod) bool) ([]*v1.Pod, []*v1.Pod) {
	var nonRemovablePods, removablePods []*v1.Pod

	for _, pod := range pods {
		if !filter(pod) {
			nonRemovablePods = append(nonRemovablePods, pod)
		} else {
			removablePods = append(removablePods, pod)
		}
	}

	return nonRemovablePods, removablePods
}

func sortPodsByRisk(
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
