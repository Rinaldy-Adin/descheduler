package nodeusagevariation

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization"
)

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type LoadVariationRiskBalancingArgs struct {
	metav1.TypeMeta `json:",inline"`

	MetricsUtilization *nodeutilization.MetricsUtilization `json:"metricsUtilization,omitempty"`

	// Naming this one differently since namespaces are still
	// considered while considering resources used by pods
	// but then filtered out before eviction
	EvictableNamespaces *api.Namespaces `json:"evictableNamespaces,omitempty"`

	// evictionLimits limits the number of evictions per domain. E.g. node, namespace, total.
	EvictionLimits *api.EvictionLimits `json:"evictionLimits,omitempty"`

	// threshold percentage for filtering if a node is overloaded, defaults to 90
	// not set to 100 to capture nodes with high utilization but low variance
	RiskThreshold api.Percentage `json:"riskThreshold,omitempty"`

	// multiplier for std deviation, defaults to 1
	SafeVarianceMargin api.Percentage `json:"SafeVarianceMargin,omitempty"`

	// root power for std deviation, defaults to 2
	SafeVarianceSensitivity api.Percentage `json:"safeVarianceSensitivity,omitempty"`
}
