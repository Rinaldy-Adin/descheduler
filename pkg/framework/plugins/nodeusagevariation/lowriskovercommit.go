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

	resourceNames := []v1.ResourceName{v1.ResourceMemory}

	if handle.PrometheusClient() == nil {
		return nil, fmt.Errorf("prometheus client not initialized")
	}

	avgUsageClient := newPrometheusUsageClient(
		handle.GetPodsAssignedToNodeFunc(),
		handle.PrometheusClient(),
		perNodeMemoryAvgPromQuery,
		perPodMemoryAvgPromQuery,
	)

	stdDevUsageClient := newPrometheusUsageClient(
		handle.GetPodsAssignedToNodeFunc(),
		handle.PrometheusClient(),
		perNodeMemoryStdDevPromQuery,
		perPodMemoryStdDevPromQuery,
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
	rawAvgPodUsage, rawStdDevPodUsage := getPodUsageDistributionSnapshot(podListMap, l.avgUsageClient, l.stdDevUsageClient)
	capacities := getNodeCapacities(nodes)

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
	podUsageMap := rawUsageToPctUsageMap(rawAvgPodUsage, rawStdDevPodUsage, capacities)

	underUsedNodes, overUsedNodes := l.classifyLoadDistribution(nodesMap, usageMap, podListMap, podUsageMap, capacities)

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
		if !l.isNodeOvercommitted(nodeInfo) && !l.isNodeAboveTargetRisk(nodeInfo) {
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
		// 1. put pods that will lower limit to under capacity in front, sort by highest limit, only put until capacity is lower
		// 2. other than pods put in front by step (1), sort by usage and stdDev
		podsWithLimit := make([]*v1.Pod, 0)
		podsWithoutLimit := make([]*v1.Pod, 0)

		for _, pod := range pods {
			podLimit := getAbsPodLimit(pod)

			if podLimit.CmpInt64(0) == 1 {
				podsWithLimit = append(podsWithLimit, pod)
			} else {
				podsWithoutLimit = append(podsWithoutLimit, pod)
			}
		}

		sort.Slice(podsWithLimit, func(i, j int) bool {

			getNormLimit := func(idx int) float64 {
				usage, ok := podUsageMap[podsWithLimit[idx].Name]
				if !ok {
					return 0
				}
				u := float64(usage.avg)
				l := float64(getRelPodLimit(podsWithLimit[idx], node.node))

				return l / u
			}

			return getNormLimit(i) > getNormLimit(j)
		})

		limitToEvict := resource.NewQuantity(0, resource.BinarySI)
		limitToEvict.Add(node.limit)
		limitToEvict.Sub(node.node.Status.Capacity[v1.ResourceMemory])
		podsToEvictDueToLimit := make([]*v1.Pod, 0)
		lastIdx := 0
		for idx, pod := range podsWithLimit {
			lastIdx = idx
			if limitToEvict.CmpInt64(0) < 1 {
				break
			}

			podsToEvictDueToLimit = append(podsToEvictDueToLimit, pod)
			limitToEvict.Sub(*getAbsPodLimit(pod))
		}

		podsToSortByRisk := make([]*v1.Pod, 0)
		for i := lastIdx; i < len(podsWithLimit); i++ {
			podsToSortByRisk = append(podsToSortByRisk, podsWithLimit[i])
		}
		podsToSortByRisk = append(podsToSortByRisk, podsWithoutLimit...)

		sort.Slice(podsToSortByRisk, func(i, j int) bool {
			pi := l.calculateRiskFromPercentage(podUsageMap[podsToSortByRisk[i].Name].avg, podUsageMap[podsToSortByRisk[i].Name].stdDev)
			pj := l.calculateRiskFromPercentage(podUsageMap[podsToSortByRisk[j].Name].avg, podUsageMap[podsToSortByRisk[j].Name].stdDev)

			return pi > pj
		})

		pods = append(podsToEvictDueToLimit, podsToSortByRisk...)
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
	podUsageMap map[string]ResourceUsageDistributions,
	capacityMap map[string]resource.Quantity,
) (
	[]NodeDistributionInfo,
	[]NodeDistributionInfo,
) {

	var (
		underUsedNodes = make([]NodeDistributionInfo, 0)
		overUsedNodes  = make([]NodeDistributionInfo, 0)
	)

	for nodeName, podList := range podListMap {
		nodeIsHighRisk := false

		mu := usageMap[nodeName].avg
		sigma := usageMap[nodeName].stdDev

		nodeAbsLimits := resource.NewQuantity(0, resource.BinarySI)
		var nodeRelLimits api.Percentage
		for _, pod := range podList {
			nodeRelLimits += getRelPodLimit(pod, nodeMap[nodeName])
			nodeAbsLimits.Add(*getAbsPodLimit(pod))
		}

		if nodeRelLimits > 100. {
			for _, pod := range podList {
				podRequest := getRelPodRequest(pod, nodeMap[nodeName])
				podLimit := getRelPodLimit(pod, nodeMap[nodeName])
				if podLimit == 0 || podRequest < podUsageMap[pod.Name].avg {
					nodeIsHighRisk = true
					break
				}
			}
		}

		risk := l.calculateRiskFromPercentage(mu, sigma)
		if risk > l.args.RiskThreshold {
			nodeIsHighRisk = true
		}

		// TODO: threshold args
		var sliceToAppend *[]NodeDistributionInfo
		if nodeIsHighRisk {
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
			limit:     *nodeAbsLimits,
		})
	}

	return underUsedNodes, overUsedNodes
}

func (l *LowRiskOvercommit) calculateRiskFromQuantities(avg, stdDev, capacity resource.Quantity) api.Percentage {
	return l.calculateRiskFromPercentage(
		ResourceQuantityToPercentage(avg, capacity),
		ResourceQuantityToPercentage(stdDev, capacity),
	)
}

func (l *LowRiskOvercommit) calculateRiskFromPercentage(avg, stdDev api.Percentage) api.Percentage {
	sigma := float64(stdDev)
	return api.Percentage(avg + api.Percentage(sigma))
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
	return risk > l.args.RiskThreshold
}

func (l *LowRiskOvercommit) isNodeOvercommitted(nodeInfo NodeDistributionInfo) bool {
	return nodeInfo.limit.Cmp(nodeInfo.node.Status.Capacity[v1.ResourceMemory]) > -1
}
