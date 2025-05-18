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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/descheduler/pkg/api"
)

func ValidateLoadVariationRiskBalancing(obj runtime.Object) error {
	args := obj.(*LoadVariationRiskBalancingArgs)

	if args.EvictableNamespaces != nil && len(args.EvictableNamespaces.Include) > 0 {
		return fmt.Errorf("only Exclude namespaces can be set, inclusion is not supported")
	}

	if args.MetricsUtilization == nil {
		return fmt.Errorf("MetricsUtilization args is required")
	}

	if !args.MetricsUtilization.MetricsServer {
		return fmt.Errorf("MetricsServer is required to be true")
	}

	if args.MetricsUtilization.Source != api.PrometheusMetrics {
		return fmt.Errorf("prometheus configuration is required")
	}

	if args.MetricsUtilization.Prometheus == nil || args.MetricsUtilization.Prometheus.Query == "" {
		return fmt.Errorf("prometheus query is required when metrics source is set to %q", api.PrometheusMetrics)
	}

	return nil
}
