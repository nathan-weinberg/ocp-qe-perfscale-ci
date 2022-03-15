# E2E Benchmarking CI Repo - Node Density


## Purpose

Run NodeDensity workload on a given OpenShift cluster. OpenShift cluster `kubeconfig` is fetched from given flexy job id.
Can read more at e2e-benchmark [kube-burner](https://github.com/cloud-bulldozer/benchmark-operator/blob/master/docs/kube-burner.md)

## Polarion Test Case
[OCP-41163](https://polarion.engineering.redhat.com/polarion/#/project/OSE/workitem?id=OCP-41163)

## Parameters

NODE_COUNT: number of worker nodes on your cluster  
PODS_PER_NODE: Work up to 250


### Author
Paige Rubendall <@paigerube14 on Github>