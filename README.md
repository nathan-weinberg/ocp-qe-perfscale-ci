# E2E Benchmarking CI Repo - Pod Density


## Purpose

Run Pod Density workload on a given OpenShift cluster. OpenShift cluster `kubeconfig` is fetched from given flexy job id.

## Polarion Test Case

[OCP-41155](https://polarion.engineering.redhat.com/polarion/#/project/OSE/workitem?id=OCP-41155)

## Parameters

Should run with PODS = 250 iterations * worker nodes

Suggest working up to 250, start with ~200 per worker node and validate before moving up 

### Author
Paige Rubendall <@paigerube14 on Github>