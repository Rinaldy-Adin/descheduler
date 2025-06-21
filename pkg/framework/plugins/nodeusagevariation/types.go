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
	SafeVarianceMargin api.Percentage `json:"safeVarianceMargin,omitempty"`

	// root power for std deviation, defaults to 2
	SafeVarianceSensitivity api.Percentage `json:"safeVarianceSensitivity,omitempty"`
}

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type LowRiskOvercommitArgs struct {
	metav1.TypeMeta `json:",inline"`

	// Naming this one differently since namespaces are still
	// considered while considering resources used by pods
	// but then filtered out before eviction
	EvictableNamespaces *api.Namespaces `json:"evictableNamespaces,omitempty"`

	// evictionLimits limits the number of evictions per domain. E.g. node, namespace, total.
	EvictionLimits *api.EvictionLimits `json:"evictionLimits,omitempty"`

	// threshold percentage for filtering if a node is overloaded, defaults to 90
	// not set to 100 to capture nodes with high utilization but low variance
	RiskThreshold api.Percentage `json:"riskThreshold,omitempty"`
}

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RemoveFailedPodsArgs holds arguments used to configure RemoveFailedPods plugin.
type RemovePotentialOOMArgs struct {
	metav1.TypeMeta `json:",inline"`

	Namespaces *api.Namespaces `json:"namespaces,omitempty"`

	// Percentage of Node util to assume prediction as OOM
	NodePredictionThreshold api.Percentage `json:"nodeThreshold,omitempty"`

	// Percentage of Pod Limit to start evicting memory increasing pods
	PodLimitPctThreshold api.Percentage `json:"podLimitPctThreshold,omitempty"`

	// Minimum R2 to assume pod as OOM
	CoefOfDeterThreshold float64 `json:"coefOfDeterThreshold,omitempty"`
}
