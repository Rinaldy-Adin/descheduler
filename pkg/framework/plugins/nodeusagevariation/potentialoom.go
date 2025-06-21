/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodeusagevariation

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
)

const RemovePotentialOOMPluginName = "RemovePotentialOOM"

// RemoveFailedPods evicts pods in failed status phase that match the given args criteria
type RemovePotentialOOM struct {
	handle      frameworktypes.Handle
	args        *RemovePotentialOOMArgs
	podFilter   podutil.FilterFunc
	usageClient *prometheusUsageClient
}

var _ frameworktypes.DeschedulePlugin = &RemovePotentialOOM{}

// New builds plugin from its arguments while passing a handle
func NewRemovePotentialOOM(args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	failedPodsArgs, ok := args.(*RemovePotentialOOMArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type RemoveFailedPodsArgs, got %T", args)
	}

	var includedNamespaces, excludedNamespaces sets.Set[string]
	if failedPodsArgs.Namespaces != nil {
		includedNamespaces = sets.New(failedPodsArgs.Namespaces.Include...)
		excludedNamespaces = sets.New(failedPodsArgs.Namespaces.Exclude...)
	}

	// We can combine Filter and PreEvictionFilter since for this strategy it does not matter where we run PreEvictionFilter
	podFilter, err := podutil.NewOptions().
		WithFilter(podutil.WrapFilterFuncs(handle.Evictor().Filter, handle.Evictor().PreEvictionFilter)).
		WithNamespaces(includedNamespaces).
		WithoutNamespaces(excludedNamespaces).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	//podFilter = podutil.WrapFilterFuncs(podFilter, func(pod *v1.Pod) bool {
	//if err := validateCanEvict(pod, failedPodsArgs); err != nil {
	//klog.V(4).InfoS(fmt.Sprintf("ignoring pod for eviction due to: %s", err.Error()), "pod", klog.KObj(pod))
	//return false
	//}

	//return true
	//})

	usageClient := newPrometheusUsageClient(
		handle.GetPodsAssignedToNodeFunc(),
		handle.PrometheusClient(),
		perNodeMemoryRawPromQuery,
		perPodMaxMemoryRawPromQuery,
	)

	return &RemovePotentialOOM{
		handle:      handle,
		podFilter:   podFilter,
		args:        failedPodsArgs,
		usageClient: usageClient,
	}, nil
}

// Name retrieves the plugin name
func (d *RemovePotentialOOM) Name() string {
	return RemovePotentialOOMPluginName
}

// Deschedule extension point implementation for the plugin
func (d *RemovePotentialOOM) Deschedule(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	d.usageClient.sync(ctx, nodes)
	d.usageClient.syncLinearRegression(ctx, nodes)

	for _, node := range nodes {
		klog.V(2).InfoS("Processing node", "node", klog.KObj(node))
		pods, err := podutil.ListAllPodsOnANode(node.Name, d.handle.GetPodsAssignedToNodeFunc(), d.podFilter)
		if err != nil {
			// no pods evicted as error encountered retrieving evictable Pods
			return &frameworktypes.Status{
				Err: fmt.Errorf("error listing pods on a node: %v", err),
			}
		}
		for _, pod := range pods {
			shouldEvict, err := d.shouldEvict(pod, node)
			if err != nil {
				klog.Errorf("should evict err: %v", err)
				continue
			}

			if !shouldEvict {
				continue
			}

			err = d.handle.Evictor().Evict(ctx, pod, evictions.EvictOptions{StrategyName: RemovePotentialOOMPluginName})
			if err == nil {
				continue
			}
			switch err.(type) {
			//case *evictions.EvictionNodeLimitError:
			//break loop
			//case *evictions.EvictionTotalLimitError:
			//return nil
			default:
				klog.Errorf("eviction failed: %v", err)
			}
		}
	}
	return nil
}

// validateCanEvict looks at failedPodArgs to see if pod can be evicted given the args.
func (d *RemovePotentialOOM) shouldEvict(pod *v1.Pod, node *v1.Node) (bool, error) {
	var (
		podHasLimit       bool
		podOverThreshold  bool
		nodeOverThreshold bool
	)

	podLimit := getAbsPodLimit(pod)
	if !podLimit.IsZero() {
		podHasLimit = true
	}

	podResourceNames, err := d.usageClient.podUsage(pod)
	if err != nil {
		klog.V(1).ErrorS(err, "Error getting pod usage")
		return false, err
	}
	podUsageRaw := podResourceNames[MetricResource]
	absPodLimit := getAbsPodLimit(pod)
	if !absPodLimit.IsZero() {
		podUsageByLimitPct := float64(podUsageRaw.Value()) / float64(absPodLimit.Value()) * 100.
		if strings.HasPrefix(pod.Namespace, "default") {
			klog.V(1).InfoS("Pod usage by limit", "pod", klog.KObj(pod), "usage", podUsageByLimitPct)
		}
		if podUsageByLimitPct > float64(d.args.PodLimitPctThreshold) {
			podOverThreshold = true
		}
	}

	nodeResourceNames := d.usageClient.nodeUtilization(node.Name)
	nodeUsageRaw := nodeResourceNames[MetricResource]

	if podHasLimit && podOverThreshold {
		if strings.HasPrefix(pod.Namespace, "default") {
			klog.V(1).InfoS("Calculating isPodOOM based on pod limit", "pod", klog.KObj(pod))
		}
		isOOM, err := d.isPodOOM(pod, absPodLimit)
		if err != nil {
			klog.V(1).ErrorS(err, "Error calculating OOM prediction")
			return false, err
		}
		return isOOM, nil
	}

	nodeThreshold := float64(getAbsNodeCapacity(node).Value()) * float64(d.args.NodePredictionThreshold) / 100.

	if strings.HasPrefix(pod.Namespace, "default") {
		klog.V(1).InfoS("Calculating isPodOOM based on node threshold", "pod", klog.KObj(pod), "nodeThreshold", nodeThreshold/(1024*1024))
	}

	podThresholdForNodeOOM := resource.NewQuantity(int64(nodeThreshold)-nodeUsageRaw.Value()+podUsageRaw.Value(), resource.BinarySI)

	isOOM, err := d.isPodOOM(pod, podThresholdForNodeOOM)
	if err != nil {
		klog.V(1).ErrorS(err, "Error calculating OOM prediction")
		return false, err
	}
	return isOOM, nil
}

func (d *RemovePotentialOOM) isPodOOM(pod *v1.Pod, rawUsageThreshold *resource.Quantity) (bool, error) {
	pred, err := d.usageClient.podNextMinPrediction(pod)
	if err != nil {
		klog.V(1).ErrorS(err, "Error getting pod prediction")
		return false, err
	}
	if strings.HasPrefix(pod.Namespace, "default") {
		klog.V(1).InfoS("Pod OOM Calculations for pod", "pod", klog.KObj(pod), "pred", pred.pred/(1024*1024), "r2", pred.r2, "threshold", rawUsageThreshold.Value()/(1024*1024))
	}

	return rawUsageThreshold.CmpInt64(int64(pred.pred)) < 1 &&
			pred.r2 > float64(d.args.CoefOfDeterThreshold),
		err
}
