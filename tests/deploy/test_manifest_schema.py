"""Validation structurelle des manifestes Argo rendus.

``argo lint`` n'est pas disponible partout (ni sur le poste ni dans le pod VSCode) : les
manifestes sont validés par un schéma JSON minimal (champs requis, types, noms
Kubernetes) et par des contrôles croisés (templates référencés, dépendances, gestionnaire
de fin). Si la CLI ``argo`` est présente, ``argo lint --offline`` s'y ajoute.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Dict

import pytest
import yaml

from kedro_pipeline.deploy.render import render_manifests, render_pipeline_manifests
from tests.deploy.toy import toy_config, toy_pipeline

jsonschema = pytest.importorskip("jsonschema")

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "kubernetes" / "generated"

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


def test_real_manifests_are_structurally_valid() -> None:
    check_manifests(render_manifests("cloud"))


def test_toy_manifests_are_structurally_valid() -> None:
    check_manifests(render_pipeline_manifests(toy_pipeline(), toy_config()))


def test_a_broken_dependency_is_detected() -> None:
    manifests = render_pipeline_manifests(toy_pipeline(), toy_config())
    broken = dict(manifests)
    broken["workflowtemplate.yaml"] = manifests["workflowtemplate.yaml"].replace(
        '"compute-toy.Succeeded"', '"missing-task.Succeeded"'
    )
    with pytest.raises(AssertionError):
        check_manifests(broken)


@pytest.mark.skipif(shutil.which("argo") is None, reason="CLI argo absente")
def test_argo_lint_offline() -> None:
    completed = subprocess.run(
        ["argo", "lint", "--offline", str(GENERATED)], check=False, capture_output=True, text=True, encoding="utf-8"
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
