package descheduler

import (
	"fmt"
	"os"

	"k8s.io/klog/v2"
	v1 "k8s.io/api/core/v1"
	kubeSchedulerConfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	kubeSchedulerScheme "k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	kubeSchedulerPluginConfig "sigs.k8s.io/scheduler-plugins/apis/config"

	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/nodeutilization"
)

func OverrideWithKubeSchedulerConfig(kubeSchedulerConfigFile string, deschedulerPolicy *api.DeschedulerPolicy) error {
	if kubeSchedulerConfigFile == "" {
		klog.V(1).InfoS("kube-scheduler config file not specified")
		return nil
	}

	policy, err := os.ReadFile(kubeSchedulerConfigFile)
	if err != nil {
		return fmt.Errorf("failed to read kube scheduler config file %q: %+v", kubeSchedulerConfigFile, err)
	}

	kubeSchedulerConfig, err := decodeKubeSchedulerConfig(kubeSchedulerConfigFile, policy)
	if err != nil {
		return fmt.Errorf("failed decoding kube scheduler config %q: %+v", kubeSchedulerConfigFile, err)
	}

	// override based on first profile that has TargetLoadPacking
	var tlpConfig *kubeSchedulerPluginConfig.TargetLoadPackingArgs
	tlpFound := false
	for _, profile := range kubeSchedulerConfig.Profiles {
		for _, pluginConfig := range profile.PluginConfig {
			if pluginConfig.Name == "TargetLoadPacking" {
				tlpFound = true
				tlpConfig = pluginConfig.Args.(*kubeSchedulerPluginConfig.TargetLoadPackingArgs)
				break
			}
		}

		if tlpFound {
			break
		}
	}

	// loop and override remove low node utilization configs, as will always interfere with TLP
	for idx, profile := range deschedulerPolicy.Profiles {
		var newPluginConfig []api.PluginConfig
		for _, pluginConfig := range profile.PluginConfigs {
			if pluginConfig.Name != "LowNodeUtilization" {
				newPluginConfig = append(newPluginConfig, pluginConfig)
			}
		}

		deschedulerPolicy.Profiles[idx].PluginConfigs = newPluginConfig
	}

	// loop and override all high node utilization configs
	for idx, profile := range deschedulerPolicy.Profiles {
		var newPluginConfig []api.PluginConfig
		for _, pluginConfig := range profile.PluginConfigs {
			if pluginConfig.Name == "HighNodeUtilization" {
				deschedulerArgs := pluginConfig.Args.(*nodeutilization.HighNodeUtilizationArgs)
				deschedulerArgs.Thresholds[v1.ResourceCPU] = api.Percentage(tlpConfig.TargetUtilization)
			}
			newPluginConfig = append(newPluginConfig, pluginConfig)
		}

		deschedulerPolicy.Profiles[idx].PluginConfigs = newPluginConfig
	}

	return nil
}

func decodeKubeSchedulerConfig(kubeSchedulerConfigFile string, data []byte) (*kubeSchedulerConfig.KubeSchedulerConfiguration, error) {
	obj, gvk, err := kubeSchedulerScheme.Codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	if cfgObj, ok := obj.(*kubeSchedulerConfig.KubeSchedulerConfiguration); ok {
		cfgObj.TypeMeta.APIVersion = gvk.GroupVersion().String()
		return cfgObj, nil
	}
	return nil, fmt.Errorf("couldn't decode as KubeSchedulerConfiguration, got %s: ", gvk)
}
