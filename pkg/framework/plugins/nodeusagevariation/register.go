package nodeusagevariation

import (
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	SchemeBuilder      = runtime.NewSchemeBuilder()
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	localSchemeBuilder.Register(addDefaultingFuncs)
}
