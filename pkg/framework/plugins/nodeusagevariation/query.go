package nodeusagevariation

// TODO: blm di avg
const perNodeCpuAvgPromQuery = `
label_replace(
  (
	1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1m]))
  )
	* on(instance) group_left(nodename)
	node_uname_info,
  "instance", "$1", "nodename", "(.*)"
)
`

// TODO: blm di avg
const perPodCpuAvgPromQuery = `sum by (pod) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))`

const perNodeCpuStdDevPromQuery = `
label_replace(
  (
	stddev_over_time( sum by (instance) ( rate(node_cpu_seconds_total{mode!="idle"}[1m]))[1m:])
  )
	* on(instance) group_left(nodename)
	node_uname_info,
  "instance", "$1", "nodename", "(.*)"
)
`

const perPodCpuStdDevPromQuery = `stddev_over_time( sum by (pod) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))[1m:])`

// TODO: instance labels
const perNodeMemoryAvgPromQuery = `avg_over_time(instance:node_memory_utilisation:ratio[1m])`

const perPodMemoryAvgPromQuery = `
sum by(pod, namespace, node) (
  avg_over_time(container_memory_usage_bytes{container!="", container!="POD"}[1m])
)
/
on(node)
group_left
label_replace(
  sum by(nodename) (
    node_memory_MemTotal_bytes{job="node-exporter"}
  ),
  "node", "$1", "nodename", "(.*)"
)`

const perNodeMemoryStdDevPromQuery = `stddev_over_time(instance:node_memory_utilisation:ratio[1m])`

const perPodMemoryStdDevPromQuery = `
sum by(pod, namespace, node) (
  stddev_over_time(container_memory_usage_bytes{container!="", container!="POD"}[1m])
)
/
on(node)
group_left
label_replace(
  sum by(nodename) (
    node_memory_MemTotal_bytes{job="node-exporter"}
  ),
  "node", "$1", "nodename", "(.*)"
)`

const perPodMemorySlope = `deriv(container_memory_usage_bytes{pod="abc"}[1m])`

// const perNodeMemoryRawPromQuery = `instance:node_memory_utilisation:ratio * node_memory_MemTotal_bytes`
const perNodeMemoryRawPromQuery = `avg_over_time(instance:node_memory_utilisation:ratio[1m]) * node_memory_MemTotal_bytes`

const perPodMemoryRawPromQuery = `
sum by(pod) (
  container_memory_usage_bytes{container!="", container!="POD"}
)
`

const perPodMemoryPromQuery = `container_memory_usage_bytes{container!="", container!="POD"}`
