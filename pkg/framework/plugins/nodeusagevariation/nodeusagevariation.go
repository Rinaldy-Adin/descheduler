package nodeusagevariation

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization/normalizer"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
	"sigs.k8s.io/descheduler/pkg/utils"
)

type continueEvictionCond func(NodeDistributionInfo, resource.Quantity) bool

type podSorterLowToHigh func(NodeDistributionInfo, []*v1.Pod, usageClient, usageClient)

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
	limit     resource.Quantity // limits in actual memory quantities, not 0-1
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
	podSorterLowToHigh podSorterLowToHigh,
	maxNoOfPodsToEvictPerNode *uint,
) {
	available := assessAvailableResourceInNodes(destinationNodes)
	klog.V(1).InfoS("Total capacity to be moved", usageToKeysAndValues(available)...)

	//limit := assessAvailableLimitInNodes(destinationNodes)
	//klog.V(1).InfoS("Total capacity to be moved", usageToKeysAndValues(available)...)

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
			"Evicting pods based on risk, if they have same priority, they'll be evicted based on QoS tiers",
		)

		podSorterLowToHigh(node, removablePods, avgUsageClient, stdDevUsageClient)

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
		podLimit := getAbsPodLimit(pod)

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

		subtractPodUsageFromNodeAvailability(&totalAvailableUsage, &nodeInfo, podUsage, podLimit)

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
	podLimit *resource.Quantity,
) {
	nodeInfo.avg.Sub(*podUsage)
	nodeInfo.limit.Sub(*podLimit)

	// TODO: consider to use requests instead, on max of either
	available.Sub(*podUsage)
}

func assessAvailableResourceInNodes(
	nodes []NodeDistributionInfo,
) resource.Quantity {
	available := resource.NewQuantity(0, resource.BinarySI)
	for _, node := range nodes {
		// TODO: consider to use requests instead, on max of either
		usage := node.avg

		available.Add(node.available)
		available.Sub(usage)
	}

	return *available
}

//func assessAvailableLimitInNodes(
//nodes []NodeDistributionInfo,
//) resource.Quantity {
//limit := resource.NewQuantity(0, resource.BinarySI)
//for _, node := range nodes {
//limit.Add(node.limit)

//for _, pod := range node.allPods {
//limit.Sub(*getAbsPodLimit(pod))
//}
//}

//return *limit
//}

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

// TODO: check usage map is already divided by capacity or not, make sure usage map has raw Data
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

func getPodUsageDistributionSnapshot(
	podListMap map[string][]*v1.Pod,
	avgUsageClient usageClient,
	stdDevUsageClient usageClient,
) (
	map[string]resource.Quantity,
	map[string]resource.Quantity,
) {
	rawAvgUsage := make(map[string]resource.Quantity)
	rawStdDevUsage := make(map[string]resource.Quantity)

	for _, pods := range podListMap {
		for _, pod := range pods {
			avgUsageList, err := avgUsageClient.podUsage(pod)
			if err != nil {
				continue
			}
			rawAvgUsage[pod.Name] = *avgUsageList[MetricResource]

			stdDevUsageList, err := stdDevUsageClient.podUsage(pod)
			if err != nil {
				continue
			}
			rawStdDevUsage[pod.Name] = *stdDevUsageList[MetricResource]
		}
	}

	return rawAvgUsage, rawStdDevUsage
}

func getNodeCapacities(nodes []*v1.Node) map[string]resource.Quantity {
	capacities := map[string]resource.Quantity{}
	for _, node := range nodes {
		capacities[node.Name] = *resource.NewQuantity(1, resource.DecimalSI)
	}
	return capacities
}

func ResourceQuantityToPercentage(
	value, total resource.Quantity,
) api.Percentage {
	return api.Percentage(float64(value.MilliValue()) / float64(total.MilliValue()) * 100.)
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

func quantityMapsToKeysAndValues(quantityMap map[string]resource.Quantity) []any {
	keysAndValues := []any{}
	for nodeName, qty := range quantityMap {
		keysAndValues = append(keysAndValues, nodeName, qty.MilliValue())
	}
	return keysAndValues
}

func usageMapToKeysAndValues(usageMap map[string]ResourceUsageDistributions) []any {
	keysAndValues := []any{}
	for nodeName, usage := range usageMap {
		keysAndValues = append(keysAndValues, "avg-"+nodeName, usage.avg, "stdDev-"+nodeName, usage.stdDev)
	}
	return keysAndValues
}

func getAbsPodLimit(pod *v1.Pod) *resource.Quantity {
	podLimit := resource.NewMilliQuantity(0, resource.BinarySI)

	if pod.Spec.Resources != nil && pod.Spec.Resources.Limits != nil {
		if limit, exists := pod.Spec.Resources.Limits[v1.ResourceMemory]; !exists {
			podLimit.Add(limit)
		}
	}

	return podLimit
}

func getRelPodLimit(pod *v1.Pod, node *v1.Node) api.Percentage {
	absPodLimit := getAbsPodLimit(pod)
	pct := api.Percentage(absPodLimit.AsApproximateFloat64())

	if capacity, ok := node.Status.Capacity[v1.ResourceMemory]; ok {
		pct /= api.Percentage(capacity.AsApproximateFloat64())
		pct *= 100.
		return pct
	}

	return pct
}

func getAbsPodRequests(pod *v1.Pod) *resource.Quantity {
	podRequest := resource.NewMilliQuantity(0, resource.BinarySI)

	if pod.Spec.Resources != nil && pod.Spec.Resources.Requests != nil {
		if request, exists := pod.Spec.Resources.Requests[v1.ResourceMemory]; !exists {
			podRequest.Add(request)
		}
	}

	return podRequest
}

func getRelPodRequest(pod *v1.Pod, node *v1.Node) api.Percentage {
	absPodRequest := getAbsPodRequests(pod)
	pct := api.Percentage(absPodRequest.AsApproximateFloat64())

	if capacity, ok := node.Status.Capacity[v1.ResourceMemory]; ok {
		pct /= api.Percentage(capacity.AsApproximateFloat64())
		pct *= 100.
		return pct
	}

	return pct
}
