# E2E Benchmarking CI Repo - Maximum Services


## Purpose

Run Max-Services workload on a given OpenShift cluster. OpenShift cluster `kubeconfig` is fetched from given flexy job id.
It creates n-replicas of an application deployment (hello-openshift) and a service in a single namespace as defined by the environment variable `SERVICE_COUNT`.
Can read more at e2e-benchmark [kube-burner](https://github.com/cloud-bulldozer/benchmark-operator/blob/master/docs/kube-burner.md)

## Polarion Test Case
[OCP-41166](https://polarion.engineering.redhat.com/polarion/#/project/OSE/workitem?id=OCP-41166)

## Parameters

SERVICE_COUNT: for a 3 workers cluster, can run 600 without error. Running with 1000, 300+ pod can't start due to "Too many pods".
(Aka 200/250 services per worker node should work without error )


### Author
Qiujie Li <@qiliRedHat on Github> 