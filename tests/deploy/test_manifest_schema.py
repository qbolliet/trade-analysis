"""Manifestes Argo du chart Helm ``trade-pipeline`` : structure et contenu.

Les manifestes sont produits par ``helm template`` (tests ignorés sans binaire ``helm``)
à partir des fichiers de données du DAG. ``argo lint`` n'est pas disponible partout : ils
sont validés par un schéma JSON minimal (champs requis, types, noms Kubernetes) et par des
contrôles croisés (templates référencés, dépendances, gestionnaire de fin). Si la CLI
``argo`` est présente, ``argo lint --offline`` s'y ajoute. La validation complète contre les
CRD (kubeconform) est faite par la CI des charts.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

jsonschema = pytest.importorskip("jsonschema")

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "helm" / "trade-pipeline"
SECRETS = {"trade-s3-credentials", "comtrade-api-credentials", "trade-postgres-credentials", "trade-mlflow-credentials"}

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="binaire helm absent")

# Nom DNS-1123 (ressources, templates, tâches)
DNS_NAME = {"type": "string", "pattern": r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", "maxLength": 63}
PARAMETERS = {
    "type": "array",
    "items": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}, "value": {"type": "string"}}},
}
METADATA = {
    "type": "object",
    "required": ["name", "namespace"],
    "properties": {"name": DNS_NAME, "namespace": DNS_NAME, "labels": {"type": "object", "additionalProperties": {"type": "string"}}},
}
ARGUMENTS = {"type": "object", "required": ["parameters"], "properties": {"parameters": PARAMETERS}}
TEMPLATE = {
    "type": "object",
    "required": ["name"],
    "properties": {
        "name": DNS_NAME,
        "inputs": {"type": "object", "properties": {"parameters": PARAMETERS}},
        "synchronization": {
            "type": "object",
            "required": ["mutex"],
            "properties": {"mutex": {"type": "object", "required": ["name"], "properties": {"name": DNS_NAME}}},
        },
        "dag": {
            "type": "object",
            "required": ["tasks"],
            "properties": {
                "tasks": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["name", "template", "arguments"],
                        "properties": {"name": DNS_NAME, "template": DNS_NAME, "depends": {"type": "string"}, "arguments": ARGUMENTS},
                        "additionalProperties": False,
                    },
                }
            },
        },
        "steps": {"type": "array", "items": {"type": "array", "items": {"type": "object", "required": ["name", "template"]}}},
        "container": {
            "type": "object",
            "required": ["image", "command", "args"],
            "properties": {
                "image": {"type": "string"},
                "command": {"type": "array", "items": {"type": "string"}},
                "args": {"type": "array", "items": {"type": "string"}},
                "env": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["name"],
                        "properties": {"name": {"type": "string"}, "value": {"type": "string"}},
                        "oneOf": [{"required": ["value"]}, {"required": ["valueFrom"]}],
                    },
                },
            },
        },
        "retryStrategy": {"type": "object", "required": ["limit"]},
        "podSpecPatch": {"type": "string"},
    },
    "oneOf": [{"required": ["dag"]}, {"required": ["steps"]}, {"required": ["container"]}],
}
WORKFLOW_TEMPLATE_SCHEMA = {
    "type": "object",
    "required": ["apiVersion", "kind", "metadata", "spec"],
    "properties": {
        "apiVersion": {"const": "argoproj.io/v1alpha1"},
        "kind": {"const": "WorkflowTemplate"},
        "metadata": METADATA,
        "spec": {
            "type": "object",
            "required": ["entrypoint", "templates", "serviceAccountName"],
            "properties": {
                "entrypoint": DNS_NAME,
                "onExit": DNS_NAME,
                "activeDeadlineSeconds": {"type": "integer", "minimum": 1},
                "parallelism": {"type": "integer", "minimum": 1},
                "arguments": ARGUMENTS,
                "templates": {"type": "array", "items": TEMPLATE},
            },
        },
    },
}
CRON_WORKFLOW_SCHEMA = {
    "type": "object",
    "required": ["apiVersion", "kind", "metadata", "spec"],
    "properties": {
        "apiVersion": {"const": "argoproj.io/v1alpha1"},
        "kind": {"const": "CronWorkflow"},
        "metadata": METADATA,
        "spec": {
            "type": "object",
            "required": ["timezone", "concurrencyPolicy", "workflowSpec"],
            "properties": {
                "schedules": {"type": "array", "minItems": 1, "items": {"type": "string", "pattern": r"^(\S+ ){4}\S+$"}},
                "schedule": {"type": "string", "pattern": r"^(\S+ ){4}\S+$"},
                "concurrencyPolicy": {"enum": ["Allow", "Forbid", "Replace"]},
                "workflowSpec": {
                    "type": "object",
                    "required": ["workflowTemplateRef", "entrypoint"],
                    "properties": {
                        "workflowTemplateRef": {"type": "object", "required": ["name"], "properties": {"name": DNS_NAME}},
                        "entrypoint": DNS_NAME,
                        "activeDeadlineSeconds": {"type": "integer", "minimum": 1},
                    },
                },
            },
            "oneOf": [{"required": ["schedules"]}, {"required": ["schedule"]}],
        },
    },
}
# Référence d'une tâche dans une expression `depends` (avant .Succeeded, .Failed…)
DEPENDS_TASK = re.compile(r"([a-z0-9-]+)\.(?:Succeeded|Failed|Errored|Skipped|Omitted|Daemoned)")


def helm_template(*options: str) -> Dict[str, str]:
    """Rend le chart (valeurs de production sauf options) : texte YAML par ressource Argo.

    Les clés reprennent les noms des anciens fichiers : ``workflowtemplate.yaml``,
    ``cronworkflow-<point d'entrée>.yaml``.
    """
    completed = subprocess.run(
        ["helm", "template", "trade", str(CHART), "--namespace", "ns", *options],
        check=True, capture_output=True, text=True, encoding="utf-8",
    )
    manifests = {}
    for document in yaml.safe_load_all(completed.stdout):
        if not document or document["kind"] not in ("WorkflowTemplate", "CronWorkflow"):
            continue
        if document["kind"] == "WorkflowTemplate":
            manifests["workflowtemplate.yaml"] = yaml.safe_dump(document)
        else:
            entry_point = document["spec"]["workflowSpec"]["entrypoint"]
            manifests[f"cronworkflow-{entry_point}.yaml"] = yaml.safe_dump(document)
    return manifests


def check_manifests(manifests: Dict[str, str]) -> None:
    """Valide le schéma et la cohérence interne d'un jeu de manifestes."""
    template = yaml.safe_load(manifests["workflowtemplate.yaml"])
    jsonschema.validate(template, WORKFLOW_TEMPLATE_SCHEMA)
    templates = {t["name"]: t for t in template["spec"]["templates"]}
    assert len(templates) == len(template["spec"]["templates"]), "noms de templates en double"
    assert template["spec"]["entrypoint"] in templates
    if "onExit" in template["spec"]:
        assert template["spec"]["onExit"] in templates
    for definition in templates.values():
        for step_group in definition.get("steps", []):
            for step in step_group:
                assert step["template"] in templates
        if "dag" in definition:
            names = {task["name"] for task in definition["dag"]["tasks"]}
            assert len(names) == len(definition["dag"]["tasks"]), "noms de tâches en double"
            for task in definition["dag"]["tasks"]:
                assert task["template"] in templates
                referenced = set(DEPENDS_TASK.findall(task.get("depends", "")))
                assert referenced <= names - {task["name"]}, (task["name"], referenced - names)
    for name, text in manifests.items():
        if name.startswith("cronworkflow-"):
            cron = yaml.safe_load(text)
            jsonschema.validate(cron, CRON_WORKFLOW_SCHEMA)
            assert cron["spec"]["workflowSpec"]["workflowTemplateRef"]["name"] == template["metadata"]["name"]
            assert cron["spec"]["workflowSpec"]["entrypoint"] in templates
            assert "dag" in templates[cron["spec"]["workflowSpec"]["entrypoint"]]


@pytest.fixture(scope="module")
def production() -> Dict[str, str]:
    return helm_template()


@pytest.fixture(scope="module")
def template(production) -> Dict[str, Any]:
    return yaml.safe_load(production["workflowtemplate.yaml"])


def _container(template: Dict[str, Any]) -> Dict[str, Any]:
    return {t["name"]: t for t in template["spec"]["templates"]}["kedro"]["container"]


@pytest.mark.parametrize("env", ["cloud", "demo"])
def test_manifests_are_structurally_valid(env: str) -> None:
    check_manifests(helm_template("--set", f"kedroEnv={env}"))


def test_a_broken_dependency_is_detected(production) -> None:
    broken = dict(production)
    broken["workflowtemplate.yaml"] = production["workflowtemplate.yaml"].replace(
        "prepare-baci.Succeeded", "missing-task.Succeeded", 1
    )
    with pytest.raises(AssertionError):
        check_manifests(broken)


def test_pod_environment_and_secrets(template) -> None:
    container = _container(template)
    env = {item["name"]: item for item in container["env"]}
    secrets = {item["valueFrom"]["secretKeyRef"]["name"] for item in env.values() if "secretKeyRef" in item.get("valueFrom", {})}
    secrets |= {item["secretRef"]["name"] for item in container["envFrom"]}
    assert secrets == SECRETS
    assert env["KEDRO_ENV"]["value"] == "cloud"
    assert env["MLFLOW_EXPERIMENT_NAME"]["value"] == "{{inputs.parameters.experiment}}"
    assert env["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"]["value"] == "true"
    assert env["MLFLOW_SYSTEM_METRICS_SAMPLING_INTERVAL"]["value"] == "30"
    assert env["AWS_SESSION_TOKEN"]["value"] == ""
    # Commande projet (kedro run ignorerait --params) et séparateur des forçages
    assert container["command"] == ["python", "-m", "kedro_pipeline"]
    assert "--env=cloud" in container["args"] and "--pipeline=__default__" in container["args"]
    params = next(arg for arg in container["args"] if arg.startswith("--params="))
    assert "runtime.FORCE_STEPS={{workflow.parameters.force-steps}}" in params


def test_workflow_parameters_and_spec(template) -> None:
    spec = template["spec"]
    parameters = {p["name"]: p["value"] for p in spec["arguments"]["parameters"]}
    assert set(parameters) == {"image-tag", "force-steps", "force-metrics", "force-methods", "extra-params"}
    assert parameters["force-steps"] == parameters["force-metrics"] == parameters["force-methods"] == ""
    assert (spec["serviceAccountName"], spec["parallelism"]) == ("workflow", 8)
    # Un seul run à la fois, toutes cadences confondues
    assert spec["synchronization"] == {"mutex": {"name": "trade-pipeline-run"}}
    assert spec["activeDeadlineSeconds"] == 518400


def test_crons(production) -> None:
    daily = yaml.safe_load(production["cronworkflow-daily.yaml"])["spec"]
    weekly = yaml.safe_load(production["cronworkflow-weekly.yaml"])["spec"]
    assert daily["schedules"] == ["0 1 * * *"] and weekly["schedules"] == ["0 2 * * 6"]
    assert daily["workflowSpec"]["entrypoint"] == "daily" and daily["workflowSpec"]["activeDeadlineSeconds"] == 82800
    assert weekly["workflowSpec"]["entrypoint"] == "weekly" and weekly["workflowSpec"]["activeDeadlineSeconds"] == 518400
    assert daily["concurrencyPolicy"] == weekly["concurrencyPolicy"] == "Forbid"
    assert daily["suspend"] is False
    # Les CronWorkflow n'exposent aucun forçage
    assert "arguments" not in daily["workflowSpec"] and "arguments" not in weekly["workflowSpec"]


def test_deployment_values_override_the_defaults() -> None:
    manifests = helm_template(
        "--set", "kedroEnv=demo", "--set", "argo.version=3.5.8", "--set", "schedules.suspend=true",
        "--set", "schedules.entryPoints.daily.cron=30 0 * * *", "--set", "image.tag=sha-1a2b3c4",
    )
    daily = yaml.safe_load(manifests["cronworkflow-daily.yaml"])["spec"]
    # Contrôleur antérieur à 3.6 : une seule planification
    assert daily["schedule"] == "30 0 * * *" and "schedules" not in daily and daily["suspend"] is True
    container = _container(yaml.safe_load(manifests["workflowtemplate.yaml"]))
    assert "--env=demo" in container["args"]
    # Étiquette immuable : pas de téléchargement systématique
    assert container["imagePullPolicy"] == "IfNotPresent"


def test_an_unrendered_environment_is_refused() -> None:
    completed = subprocess.run(
        ["helm", "template", "trade", str(CHART), "--set", "kedroEnv=prod"],
        capture_output=True, text=True, encoding="utf-8",
    )
    assert completed.returncode != 0 and "dag-prod.yaml" in completed.stderr


@pytest.mark.skipif(shutil.which("argo") is None, reason="CLI argo absente")
def test_argo_lint_offline(production, tmp_path) -> None:
    for name, text in production.items():
        (tmp_path / name).write_text(text, encoding="utf-8")
    completed = subprocess.run(
        ["argo", "lint", "--offline", str(tmp_path)], check=False, capture_output=True, text=True, encoding="utf-8"
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
