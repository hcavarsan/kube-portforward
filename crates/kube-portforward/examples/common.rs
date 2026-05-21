#![allow(dead_code, unreachable_pub)]

use std::time::Duration;

use anyhow::{
    Context,
    Result,
    bail,
};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    Api,
    DeleteParams,
    PostParams,
};
use kube::runtime::wait::{
    Condition,
    await_condition,
};
use serde_json::json;

/// Create nginx pod, wait up to 90s for Ready.
pub async fn ensure_nginx_pod(
    kube_client: &kube::Client, namespace: &str, name: &str,
) -> Result<String> {
    let pods: Api<Pod> = Api::namespaced(kube_client.clone(), namespace);

    if pods.get_opt(name).await?.is_none() {
        let manifest: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "labels": { "app": name, "managed-by": "kube-portforward-example" },
            },
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "image": "nginx:alpine",
                    "ports": [{ "containerPort": 80 }],
                }],
                "terminationGracePeriodSeconds": 1,
            }
        }))?;
        pods.create(&PostParams::default(), &manifest)
            .await
            .with_context(|| format!("create pod {namespace}/{name}"))?;
        println!("* created pod {namespace}/{name}, waiting for Ready");
    } else {
        println!("* pod {namespace}/{name} exists, reusing");
    }

    let ready = await_condition(pods.clone(), name, is_pod_ready());
    match tokio::time::timeout(Duration::from_secs(90), ready).await {
        Ok(Ok(_)) => {
            println!("* pod {namespace}/{name} Ready");
            Ok(name.to_string())
        }
        Ok(Err(e)) => bail!("waiting for pod ready: {e}"),
        Err(_) => bail!("pod {namespace}/{name} did not become Ready within 90s"),
    }
}

pub async fn delete_pod(kube_client: &kube::Client, namespace: &str, name: &str) {
    let pods: Api<Pod> = Api::namespaced(kube_client.clone(), namespace);
    match pods.delete(name, &DeleteParams::default()).await {
        Ok(_) => println!("* deleted pod {namespace}/{name}"),
        Err(e) => eprintln!("* delete {namespace}/{name} failed: {e}"),
    }
}

/// Delete then wait until the pod is fully gone from the
/// apiserver to simulate a pod restart.
pub async fn force_delete_and_wait(
    kube_client: &kube::Client, namespace: &str, name: &str,
) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(kube_client.clone(), namespace);
    let params = DeleteParams {
        grace_period_seconds: Some(0),
        ..Default::default()
    };
    pods.delete(name, &params)
        .await
        .with_context(|| format!("force-delete {namespace}/{name}"))?;
    println!("* force-deleted pod {namespace}/{name}, waiting for removal");
    for _ in 0..60 {
        if pods.get_opt(name).await?.is_none() {
            println!("* pod {namespace}/{name} gone");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("pod {namespace}/{name} still present after 30s");
}

fn is_pod_ready() -> impl Condition<Pod> {
    |obj: Option<&Pod>| {
        obj.and_then(|p| p.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false)
    }
}
