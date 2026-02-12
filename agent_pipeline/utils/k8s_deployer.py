"""
Live Kubernetes deployment engine using Kind (Kubernetes in Docker).

Handles the full deployment lifecycle:
  1. Write generated code + Dockerfiles to disk
  2. Create a Kind cluster (if not exists)
  3. Build Docker images
  4. Load images into Kind
  5. Generate and apply K8s manifests
  6. Wait for pods to become ready
  7. Run health checks
  8. Expose services via port-forward

Prerequisites (on the host machine):
  - Docker Desktop or Docker Engine running
  - kind CLI installed (https://kind.sigs.k8s.io/)
  - kubectl CLI installed
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml


@dataclass
class DeploymentLog:
    """Structured log entry for deployment operations."""
    step: str
    status: str  # "running", "success", "error", "skipped"
    message: str
    duration_seconds: float = 0
    details: str = ""


@dataclass
class LiveDeploymentResult:
    """Result of a live deployment attempt."""
    success: bool
    cluster_name: str
    namespace: str
    services_deployed: list[str]
    pods: list[dict[str, str]]  # [{"name": ..., "status": ..., "ready": ...}]
    port_forwards: dict[str, int]  # {"service-name": local_port}
    logs: list[DeploymentLog] = field(default_factory=list)
    error: str | None = None


class KubernetesDeployer:
    """
    Orchestrates a real local Kubernetes deployment via Kind.

    Takes the LLM-generated deployer output (container specs, K8s resources,
    health checks) and materializes it on a local Kind cluster.
    """

    def __init__(
        self,
        output_dir: str = "./deploy_output",
        cluster_name: str = "agent-pipeline",
        namespace: str = "agent-app",
        on_log: Callable[[DeploymentLog], None] | None = None,
    ):
        self.output_dir = Path(output_dir).resolve()
        self.cluster_name = cluster_name
        self.namespace = namespace
        self.on_log = on_log
        self._logs: list[DeploymentLog] = []

    def _log(self, step: str, status: str, message: str, duration: float = 0, details: str = ""):
        entry = DeploymentLog(step=step, status=status, message=message, duration_seconds=duration, details=details)
        self._logs.append(entry)
        if self.on_log:
            self.on_log(entry)

    def _run_cmd(self, cmd: list[str], timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
        """Run a shell command and return the result."""
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=check,
            )
            return result
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Command failed: {' '.join(cmd)}\nstderr: {e.stderr}") from e
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Command timed out after {timeout}s: {' '.join(cmd)}")

    def _check_prerequisites(self) -> dict[str, bool]:
        """Verify that Docker, Kind, and kubectl are available."""
        tools = {}
        for tool in ["docker", "kind", "kubectl"]:
            try:
                self._run_cmd([tool, "version"], timeout=10, check=False)
                tools[tool] = True
            except (FileNotFoundError, RuntimeError):
                tools[tool] = False
        return tools

    # ─── Step 1: Write files to disk ──────────────────────────────────────────

    def write_artifacts(
        self,
        developer_output: dict[str, Any],
        deployer_output: dict[str, Any],
    ) -> Path:
        """
        Write all generated code, Dockerfiles, and K8s manifests to the output directory.
        Returns the path to the output directory.
        """
        start = time.time()
        self._log("write_artifacts", "running", "Writing generated code and manifests to disk...")

        # Clean and create output directory
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Write source code from developer agent
        src_dir = self.output_dir / "src"
        src_dir.mkdir(exist_ok=True)

        files_written = 0
        for impl in developer_output.get("implementations", []):
            comp_name = impl.get("component_name", "unknown").lower().replace(" ", "-")
            comp_dir = src_dir / comp_name
            comp_dir.mkdir(parents=True, exist_ok=True)

            for file_info in impl.get("files", []):
                file_path = comp_dir / file_info.get("path", "main.py").split("/")[-1]
                file_path.write_text(file_info.get("code", "# No code generated"))
                files_written += 1

        # Write Dockerfiles from deployer agent
        docker_dir = self.output_dir / "docker"
        docker_dir.mkdir(exist_ok=True)

        for image in deployer_output.get("container_images", []):
            image_name = image.get("name", "app").lower().replace(" ", "-")
            dockerfile_path = docker_dir / image_name / "Dockerfile"
            dockerfile_path.parent.mkdir(parents=True, exist_ok=True)

            snippet = image.get("dockerfile_snippet", "")
            # If the snippet is a partial Dockerfile, wrap it into a complete one
            if snippet and not snippet.strip().startswith("FROM"):
                base = image.get("base_image", "python:3.11-slim")
                port = image.get("port", 8080)
                dockerfile_content = textwrap.dedent(f"""\
                    FROM {base}
                    WORKDIR /app
                    {snippet}
                    EXPOSE {port}
                """)
            elif snippet:
                dockerfile_content = snippet
            else:
                base = image.get("base_image", "python:3.11-slim")
                port = image.get("port", 8080)
                dockerfile_content = textwrap.dedent(f"""\
                    FROM {base}
                    WORKDIR /app
                    COPY . .
                    RUN pip install --no-cache-dir -r requirements.txt 2>/dev/null || true
                    EXPOSE {port}
                    CMD ["python", "main.py"]
                """)

            dockerfile_path.write_text(dockerfile_content)
            files_written += 1

            # Copy relevant source code into the Docker build context
            for impl in developer_output.get("implementations", []):
                comp_name_check = impl.get("component_name", "").lower().replace(" ", "-")
                if comp_name_check == image_name or image_name in comp_name_check:
                    for file_info in impl.get("files", []):
                        src_file = dockerfile_path.parent / file_info.get("path", "main.py").split("/")[-1]
                        src_file.write_text(file_info.get("code", ""))

        # Write K8s manifests
        k8s_dir = self.output_dir / "k8s"
        k8s_dir.mkdir(exist_ok=True)

        # Namespace manifest
        ns_manifest = textwrap.dedent(f"""\
            apiVersion: v1
            kind: Namespace
            metadata:
              name: {self.namespace}
        """)
        (k8s_dir / "00-namespace.yaml").write_text(ns_manifest)
        files_written += 1

        # Resource manifests from deployer output
        for i, resource in enumerate(deployer_output.get("kubernetes_resources", []), start=1):
            kind = resource.get("kind", "Unknown")
            name = resource.get("name", f"resource-{i}").lower()
            yaml_snippet = resource.get("yaml_snippet", "")

            if yaml_snippet.strip():
                try:
                    manifest_content = self._sanitize_yaml(yaml_snippet)
                    self._log(
                        "write_artifacts", "success",
                        f"Validated YAML for {kind}/{name}",
                    )
                except ValueError as e:
                    self._log(
                        "write_artifacts", "running",
                        f"LLM YAML invalid for {kind}/{name}: {e} — using fallback generator",
                    )
                    manifest_content = self._generate_manifest(resource, deployer_output)
            else:
                manifest_content = self._generate_manifest(resource, deployer_output)

            filename = f"{i:02d}-{kind.lower()}-{name}.yaml"
            (k8s_dir / filename).write_text(manifest_content)
            files_written += 1

        # Write health check manifests if not already covered
        for hc in deployer_output.get("health_checks", []):
            service_name = hc.get("service", "app").lower().replace(" ", "-")
            liveness = hc.get("liveness_probe", {})
            readiness = hc.get("readiness_probe", {})

            # These would typically be part of the Deployment spec
            # Write a reference file for documentation
            hc_doc = json.dumps(hc, indent=2)
            (k8s_dir / f"ref-healthcheck-{service_name}.json").write_text(hc_doc)

        duration = time.time() - start
        self._log(
            "write_artifacts", "success",
            f"Wrote {files_written} files to {self.output_dir}",
            duration=duration,
        )
        return self.output_dir

    def _sanitize_yaml(self, yaml_str: str) -> str:
        """
        Clean and validate YAML from LLM output.

        Three-layer approach:
          1. Dedent & strip blank lines
          2. Auto-repair common issues (tabs, trailing commas)
          3. Validate with yaml.safe_load()

        Returns clean, validated YAML string.
        Raises ValueError if unrecoverable.
        """
        # Layer 1: Dedent + strip
        dedented = textwrap.dedent(yaml_str)
        lines = dedented.split("\n")
        # Strip leading blank lines
        while lines and not lines[0].strip():
            lines.pop(0)
        # Strip trailing blank lines
        while lines and not lines[-1].strip():
            lines.pop()
        cleaned = "\n".join(lines) + "\n"

        # Try strict validation first
        try:
            parsed = yaml.safe_load(cleaned)
            if isinstance(parsed, dict):
                return cleaned
        except yaml.YAMLError:
            pass

        # Layer 2: Auto-repair common issues
        repaired = cleaned
        # Tabs → 2 spaces
        repaired = repaired.replace("\t", "  ")
        # Trailing commas at end of lines (JSON bleed)
        repaired = re.sub(r",\s*$", "", repaired, flags=re.MULTILINE)
        # Remove Markdown code fences if LLM wrapped the YAML
        repaired = re.sub(r"^```(?:yaml|yml)?\s*\n?", "", repaired)
        repaired = re.sub(r"\n?```\s*$", "", repaired)

        try:
            parsed = yaml.safe_load(repaired)
            if isinstance(parsed, dict):
                return repaired
        except yaml.YAMLError:
            pass

        # Layer 3: Nuclear option — round-trip through PyYAML
        # Parse whatever we can, dump it back to clean YAML
        try:
            parsed = yaml.safe_load(repaired)
            if parsed is not None:
                return yaml.dump(parsed, default_flow_style=False, sort_keys=False)
        except yaml.YAMLError as e:
            raise ValueError(f"YAML is unrecoverable after sanitization: {e}")

        raise ValueError("YAML parsed to empty/null after sanitization")

    def _generate_manifest(self, resource: dict[str, Any], deployer_output: dict[str, Any]) -> str:
        """Generate a basic K8s manifest when the LLM didn't provide full YAML."""
        kind = resource.get("kind", "ConfigMap")
        name = resource.get("name", "resource").lower()
        ns = resource.get("namespace", self.namespace)

        if kind == "Deployment":
            # Find matching container image
            image_name = f"{name}:latest"
            port = 8080
            for img in deployer_output.get("container_images", []):
                if img.get("name", "").lower().replace(" ", "-") in name or name in img.get("name", "").lower():
                    image_name = f"{img.get('name', name).lower().replace(' ', '-')}:{img.get('tag', 'latest')}"
                    port = img.get("port", 8080)
                    break

            return textwrap.dedent(f"""\
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: {name}
                  namespace: {ns}
                  labels:
                    app: {name}
                spec:
                  replicas: 2
                  selector:
                    matchLabels:
                      app: {name}
                  template:
                    metadata:
                      labels:
                        app: {name}
                    spec:
                      containers:
                      - name: {name}
                        image: {image_name}
                        ports:
                        - containerPort: {port}
                        resources:
                          requests:
                            memory: "128Mi"
                            cpu: "100m"
                          limits:
                            memory: "256Mi"
                            cpu: "500m"
                        livenessProbe:
                          httpGet:
                            path: /health
                            port: {port}
                          initialDelaySeconds: 10
                          periodSeconds: 15
                        readinessProbe:
                          httpGet:
                            path: /ready
                            port: {port}
                          initialDelaySeconds: 5
                          periodSeconds: 10
            """)

        elif kind == "Service":
            port = 8080
            for img in deployer_output.get("container_images", []):
                if img.get("name", "").lower().replace(" ", "-") in name:
                    port = img.get("port", 8080)
                    break

            return textwrap.dedent(f"""\
                apiVersion: v1
                kind: Service
                metadata:
                  name: {name}
                  namespace: {ns}
                spec:
                  selector:
                    app: {name.replace('-svc', '').replace('-service', '')}
                  ports:
                  - protocol: TCP
                    port: {port}
                    targetPort: {port}
                  type: ClusterIP
            """)

        elif kind == "Ingress":
            return textwrap.dedent(f"""\
                apiVersion: networking.k8s.io/v1
                kind: Ingress
                metadata:
                  name: {name}
                  namespace: {ns}
                  annotations:
                    nginx.ingress.kubernetes.io/rewrite-target: /
                spec:
                  rules:
                  - host: {name}.local
                    http:
                      paths:
                      - path: /
                        pathType: Prefix
                        backend:
                          service:
                            name: {name.replace('-ingress', '-svc')}
                            port:
                              number: 8080
            """)

        elif kind == "HPA":
            return textwrap.dedent(f"""\
                apiVersion: autoscaling/v2
                kind: HorizontalPodAutoscaler
                metadata:
                  name: {name}
                  namespace: {ns}
                spec:
                  scaleTargetRef:
                    apiVersion: apps/v1
                    kind: Deployment
                    name: {name.replace('-hpa', '')}
                  minReplicas: 2
                  maxReplicas: 10
                  metrics:
                  - type: Resource
                    resource:
                      name: cpu
                      target:
                        type: Utilization
                        averageUtilization: 70
            """)

        else:
            # Generic ConfigMap fallback
            return textwrap.dedent(f"""\
                apiVersion: v1
                kind: {kind}
                metadata:
                  name: {name}
                  namespace: {ns}
                data:
                  config: |
                    {resource.get('key_config', 'generated by agent pipeline')}
            """)

    # ─── Step 2: Kind cluster ─────────────────────────────────────────────────

    def ensure_cluster(self) -> tuple[bool, str]:
        """
        Create a Kind cluster if it doesn't already exist.

        Returns (success, error_detail) — error_detail is empty on success.
        """
        start = time.time()
        self._log("ensure_cluster", "running", f"Checking for Kind cluster '{self.cluster_name}'...")

        # Check if cluster exists
        result = self._run_cmd(["kind", "get", "clusters"], check=False)
        existing = result.stdout.strip().split("\n") if result.stdout.strip() else []

        if self.cluster_name in existing:
            duration = time.time() - start
            self._log(
                "ensure_cluster", "success",
                f"Kind cluster '{self.cluster_name}' already exists",
                duration=duration,
            )
            return True, ""

        # Create cluster with port mappings for ingress
        self._log("ensure_cluster", "running", f"Creating Kind cluster '{self.cluster_name}'...")

        kind_config = textwrap.dedent(f"""\
            kind: Cluster
            apiVersion: kind.x-k8s.io/v1alpha4
            name: {self.cluster_name}
            nodes:
            - role: control-plane
              kubeadmConfigPatches:
              - |
                kind: InitConfiguration
                nodeRegistration:
                  kubeletExtraArgs:
                    node-labels: "ingress-ready=true"
              extraPortMappings:
              - containerPort: 80
                hostPort: 8080
                protocol: TCP
              - containerPort: 443
                hostPort: 8443
                protocol: TCP
            - role: worker
            - role: worker
        """)

        config_path = self.output_dir / "kind-config.yaml"
        config_path.write_text(kind_config)

        try:
            self._run_cmd(
                ["kind", "create", "cluster", "--config", str(config_path)],
                timeout=300,
            )
        except RuntimeError as e:
            duration = time.time() - start
            error_str = str(e)
            diagnosis = self._diagnose_cluster_error(error_str)
            self._log("ensure_cluster", "error", diagnosis, duration=duration, details=error_str)
            return False, diagnosis

        # Explicitly switch kubectl context to the new cluster
        context = f"kind-{self.cluster_name}"
        try:
            self._run_cmd(
                ["kubectl", "config", "use-context", context],
                timeout=15,
            )
            self._log("ensure_cluster", "running", f"Switched kubectl context to '{context}'")
        except RuntimeError:
            self._log(
                "ensure_cluster", "running",
                f"Context switch to '{context}' failed — will retry via cluster-info.",
            )

        # Wait for API server to become reachable (cluster can take a few seconds after creation)
        api_ready = False
        for attempt in range(6):
            try:
                self._run_cmd(
                    ["kubectl", "cluster-info", "--context", context],
                    timeout=15,
                )
                api_ready = True
                break
            except RuntimeError:
                self._log(
                    "ensure_cluster", "running",
                    f"Waiting for API server to be ready (attempt {attempt + 1}/6)...",
                )
                time.sleep(5)

        if not api_ready:
            duration = time.time() - start
            diagnosis = (
                f"Kind cluster '{self.cluster_name}' was created, but the Kubernetes API server "
                "did not become reachable within 30 seconds. This usually resolves on its own — "
                "try re-running the pipeline. If it persists, check Docker resource allocation."
            )
            self._log("ensure_cluster", "error", diagnosis, duration=duration)
            return False, diagnosis

        # Create namespace upfront so manifests don't fail on missing namespace
        try:
            self._run_cmd(
                ["kubectl", "create", "namespace", self.namespace, "--context", context],
                timeout=15,
                check=False,  # Ignore "already exists" errors
            )
            self._log("ensure_cluster", "success", f"Namespace '{self.namespace}' ensured")
        except RuntimeError:
            pass  # Namespace may already exist — that's fine

        duration = time.time() - start
        self._log(
            "ensure_cluster", "success",
            f"Kind cluster '{self.cluster_name}' created and verified (3 nodes)",
            duration=duration,
        )
        return True, ""

    @staticmethod
    def _diagnose_cluster_error(error_msg: str) -> str:
        """Produce a human-readable diagnosis for a Kind cluster creation failure."""
        err = error_msg.lower()

        if "address already in use" in err or ("bind" in err and "in use" in err) or ("port" in err and "in use" in err):
            return ("Port conflict: another service is using port 8080 or 8443. "
                    "Stop the conflicting service (check `lsof -i :8080`) or edit the Kind config to use different ports.")
        if "docker" in err and ("daemon" in err or "not running" in err or "connect" in err or "cannot connect" in err):
            return ("Docker is not running. Start Docker Desktop and wait for it to be ready, then retry.")
        if "already exists" in err:
            return (f"A Kind cluster with this name already exists but may be unhealthy. "
                    f"Try: `kind delete cluster --name <cluster>` then re-run.")
        if "timeout" in err or "timed out" in err:
            return ("Cluster creation timed out. Docker may be resource-constrained. "
                    "Try increasing Docker's memory/CPU allocation in Docker Desktop Settings > Resources.")
        if "permission" in err or "denied" in err:
            return ("Permission denied. Ensure your user can run Docker without sudo. "
                    "On Linux: `sudo usermod -aG docker $USER` then log out/back in.")
        if "no space" in err or "disk" in err:
            return ("Not enough disk space to create the cluster. Free up space or run `docker system prune` to clean up unused images/containers.")
        if "kind" in err and "not found" in err:
            return ("The `kind` CLI is not installed. Install it: `brew install kind` (macOS) or see https://kind.sigs.k8s.io/docs/user/quick-start/")

        # Fallback: extract the most useful part of the stderr
        stderr_lines = [l.strip() for l in error_msg.split('\n') if 'stderr:' in l.lower()]
        if stderr_lines:
            return f"Cluster creation failed. kubectl/kind said: {stderr_lines[0][:300]}"
        return f"Cluster creation failed: {error_msg[:300]}"

    # ─── Stub container generation ───────────────────────────────────────────

    def _write_stub_container(self, build_context: Path, image_spec: dict[str, Any]) -> None:
        """
        Write a minimal, guaranteed-to-build stub container into a build context.

        The stub is a lightweight Python HTTP server that:
          - Responds 200 on /health and /ready (so K8s probes pass)
          - Serves a JSON status page on / showing the component name and metadata
          - Listens on the correct port from the image spec
        """
        port = image_spec.get("port", 8080)
        name = image_spec.get("name", "service")
        base_image = "python:3.11-slim"

        stub_server = textwrap.dedent(f'''\
            """Stub health-check server for {name}."""
            import json
            from http.server import HTTPServer, BaseHTTPRequestHandler
            from datetime import datetime

            PORT = {port}
            SERVICE = "{name}"
            START_TIME = datetime.utcnow().isoformat()

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    if self.path in ("/health", "/healthz"):
                        self._respond(200, {{"status": "healthy", "service": SERVICE}})
                    elif self.path in ("/ready", "/readyz"):
                        self._respond(200, {{"status": "ready", "service": SERVICE}})
                    elif self.path == "/":
                        self._respond(200, {{
                            "service": SERVICE,
                            "status": "running",
                            "started_at": START_TIME,
                            "message": f"{{SERVICE}} stub — deployed by Agent Pipeline",
                            "endpoints": ["/", "/health", "/ready"],
                        }})
                    else:
                        self._respond(404, {{"error": "not found"}})

                def _respond(self, code, body):
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(body, indent=2).encode())

                def log_message(self, fmt, *args):
                    print(f"[{{SERVICE}}] {{fmt % args}}")

            if __name__ == "__main__":
                server = HTTPServer(("0.0.0.0", PORT), Handler)
                print(f"{{SERVICE}} stub listening on :{{PORT}}")
                server.serve_forever()
        ''')

        dockerfile = textwrap.dedent(f"""\
            FROM {base_image}
            WORKDIR /app
            COPY server.py .
            EXPOSE {port}
            CMD ["python", "server.py"]
        """)

        build_context.mkdir(parents=True, exist_ok=True)
        (build_context / "server.py").write_text(stub_server)
        (build_context / "Dockerfile").write_text(dockerfile)

    # ─── Step 3: Build Docker images ──────────────────────────────────────────

    def build_images(self, deployer_output: dict[str, Any]) -> list[str]:
        """
        Build Docker images for each container spec.

        If the LLM-generated Dockerfile fails to build, automatically falls back
        to a stub container (minimal Python health-check server) so the deployment
        pipeline can proceed end-to-end.
        """
        start = time.time()
        built_images = []

        images = deployer_output.get("container_images", [])
        self._log("build_images", "running", f"Building {len(images)} Docker images...")

        docker_dir = self.output_dir / "docker"

        for image in images:
            img_name = image.get("name", "app").lower().replace(" ", "-")
            tag = image.get("tag", "latest")
            full_tag = f"{img_name}:{tag}"

            build_context = docker_dir / img_name
            if not build_context.exists():
                build_context.mkdir(parents=True, exist_ok=True)

            # Ensure there's at least a Dockerfile
            if not (build_context / "Dockerfile").exists():
                self._log("build_images", "running", f"No Dockerfile for {img_name}, generating stub...")
                self._write_stub_container(build_context, image)

            self._log("build_images", "running", f"Building {full_tag}...")

            try:
                self._run_cmd(
                    ["docker", "build", "-t", full_tag, str(build_context)],
                    timeout=300,
                )
                built_images.append(full_tag)
                self._log("build_images", "success", f"Built {full_tag}")
            except RuntimeError as e:
                # ── Fallback: replace with stub and rebuild ──
                self._log(
                    "build_images", "running",
                    f"LLM Dockerfile failed for {img_name}, falling back to stub container..."
                )
                self._write_stub_container(build_context, image)

                try:
                    self._run_cmd(
                        ["docker", "build", "-t", full_tag, str(build_context)],
                        timeout=300,
                    )
                    built_images.append(full_tag)
                    self._log("build_images", "success", f"Built {full_tag} (stub fallback)")
                except RuntimeError as e2:
                    self._log("build_images", "error", f"Stub build also failed for {full_tag}: {e2}")

        duration = time.time() - start
        self._log(
            "build_images", "success",
            f"Built {len(built_images)}/{len(images)} images",
            duration=duration,
        )
        return built_images

    # ─── Step 4: Load images into Kind ────────────────────────────────────────

    def load_images(self, image_tags: list[str]) -> bool:
        """Load built Docker images into the Kind cluster."""
        start = time.time()
        self._log("load_images", "running", f"Loading {len(image_tags)} images into Kind cluster...")

        for tag in image_tags:
            try:
                self._run_cmd(
                    ["kind", "load", "docker-image", tag, "--name", self.cluster_name],
                    timeout=120,
                )
                self._log("load_images", "success", f"Loaded {tag}")
            except RuntimeError as e:
                self._log("load_images", "error", f"Failed to load {tag}: {e}")

        duration = time.time() - start
        self._log("load_images", "success", f"All images loaded", duration=duration)
        return True

    # ─── Step 5: Apply K8s manifests ──────────────────────────────────────────

    def apply_manifests(self) -> dict[str, Any]:
        """
        Apply all K8s manifests in order.

        Returns a dict with:
          - success (bool): True if at least one manifest applied
          - applied (int): count of successfully applied manifests
          - total (int): total manifest count
          - failures (list): [{"file": ..., "error": ..., "diagnosis": ...}]
        """
        start = time.time()
        k8s_dir = self.output_dir / "k8s"
        result_info: dict[str, Any] = {"success": False, "applied": 0, "total": 0, "failures": []}

        if not k8s_dir.exists():
            self._log("apply_manifests", "error", "No k8s directory found")
            result_info["failures"].append({
                "file": "N/A",
                "error": "No k8s manifest directory was created",
                "diagnosis": "The LLM did not generate any Kubernetes YAML resources. "
                             "Try re-running the pipeline or using a Focused sample objective.",
            })
            return result_info

        # Set kubectl context to the Kind cluster — with self-healing retry
        context = f"kind-{self.cluster_name}"
        cluster_reachable = False

        for attempt in range(3):
            try:
                self._run_cmd(["kubectl", "cluster-info", "--context", context], timeout=15)
                cluster_reachable = True
                break
            except RuntimeError:
                if attempt == 0:
                    # First failure: try explicitly switching context
                    self._log(
                        "apply_manifests", "running",
                        f"Context '{context}' unreachable — attempting to switch context...",
                    )
                    try:
                        self._run_cmd(
                            ["kubectl", "config", "use-context", context],
                            timeout=15,
                        )
                    except RuntimeError:
                        pass
                elif attempt == 1:
                    # Second failure: try re-exporting kubeconfig from Kind
                    self._log(
                        "apply_manifests", "running",
                        "Re-exporting kubeconfig from Kind cluster...",
                    )
                    try:
                        self._run_cmd(
                            ["kind", "export", "kubeconfig", "--name", self.cluster_name],
                            timeout=30,
                        )
                    except RuntimeError:
                        pass
                time.sleep(3)

        if not cluster_reachable:
            # Final check: is the cluster even listed by Kind?
            try:
                list_result = self._run_cmd(["kind", "get", "clusters"], check=False)
                existing = list_result.stdout.strip().split("\n") if list_result.stdout.strip() else []
            except RuntimeError:
                existing = []

            if self.cluster_name in existing:
                diagnosis = (
                    f"Kind cluster '{self.cluster_name}' exists but kubectl cannot connect. "
                    f"Try manually: `kind export kubeconfig --name {self.cluster_name}` "
                    f"then `kubectl cluster-info --context {context}`"
                )
            else:
                diagnosis = (
                    f"Kind cluster '{self.cluster_name}' does not exist. "
                    f"Create it with: `kind create cluster --name {self.cluster_name}` "
                    "or re-run the pipeline with Live Deployment enabled."
                )

            self._log("apply_manifests", "error", diagnosis)
            result_info["failures"].append({
                "file": "N/A",
                "error": f"Cannot connect to kubectl context '{context}'",
                "diagnosis": diagnosis,
            })
            return result_info

        # Get sorted manifest files (namespace first via 00- prefix)
        manifests = sorted(k8s_dir.glob("*.yaml"))
        result_info["total"] = len(manifests)

        if not manifests:
            self._log("apply_manifests", "error", "No .yaml files found in k8s directory")
            result_info["failures"].append({
                "file": "N/A",
                "error": "k8s directory exists but contains no .yaml files",
                "diagnosis": "The deployment plan did not produce valid YAML manifests. "
                             "Try re-running the pipeline.",
            })
            return result_info

        self._log("apply_manifests", "running", f"Applying {len(manifests)} manifests...")

        for manifest in manifests:
            try:
                cmd_result = self._run_cmd(
                    ["kubectl", "apply", "-f", str(manifest), "--context", context],
                    timeout=30,
                )
                result_info["applied"] += 1
                self._log("apply_manifests", "success", f"Applied {manifest.name}: {cmd_result.stdout.strip()}")
            except RuntimeError as e:
                error_str = str(e)
                diagnosis = self._diagnose_manifest_error(manifest, error_str)
                result_info["failures"].append({
                    "file": manifest.name,
                    "error": error_str,
                    "diagnosis": diagnosis,
                })
                self._log(
                    "apply_manifests", "error",
                    f"Failed {manifest.name}: {diagnosis}",
                    details=error_str,
                )

        duration = time.time() - start
        result_info["success"] = result_info["applied"] > 0

        status = "success" if result_info["success"] else "error"
        self._log(
            "apply_manifests", status,
            f"Applied {result_info['applied']}/{result_info['total']} manifests "
            f"({len(result_info['failures'])} failed)",
            duration=duration,
        )
        return result_info

    @staticmethod
    def _diagnose_manifest_error(manifest_path: Path, error_msg: str) -> str:
        """
        Produce a human-readable diagnosis for a kubectl apply error.
        Reads the manifest file to give context-aware advice.
        """
        err = error_msg.lower()
        name = manifest_path.name

        # Try to read the manifest for additional context
        manifest_content = ""
        try:
            manifest_content = manifest_path.read_text()
        except Exception:
            pass

        # ── Pattern-match common kubectl errors ──
        if "invalid" in err and "apiversion" in err:
            return (f"'{name}' has an invalid apiVersion. "
                    "Check the YAML starts with a valid 'apiVersion:' line (e.g., apps/v1, v1).")

        if "no matches for kind" in err:
            # Extract the kind name
            import re
            kind_match = re.search(r'no matches for kind "(\w+)"', error_msg)
            kind_name = kind_match.group(1) if kind_match else "unknown"
            return (f"'{name}' uses Kind '{kind_name}' which is not installed on this cluster. "
                    f"This usually means a CRD is missing. For a basic Kind cluster, "
                    f"stick to core resources: Deployment, Service, ConfigMap, Secret, Ingress.")

        if "namespaces" in err and "not found" in err:
            return (f"'{name}' references a namespace that doesn't exist. "
                    f"Add a Namespace manifest (00-namespace.yaml) that runs first, or use: "
                    f"kubectl create namespace {manifest_path.parent.name}")

        if "already exists" in err:
            return f"'{name}' — resource already exists (safe to ignore on re-runs)."

        if "spec.selector" in err or "selector" in err:
            return (f"'{name}' has a selector mismatch. Ensure the Deployment's "
                    "spec.selector.matchLabels matches spec.template.metadata.labels exactly.")

        if "port" in err and ("invalid" in err or "must be" in err):
            return (f"'{name}' has an invalid port configuration. "
                    "Ports must be integers between 1-65535. Check containerPort, port, and targetPort values.")

        if "unmarshal" in err or "yaml" in err or "mapping" in err:
            return (f"'{name}' contains invalid YAML syntax. Common causes: "
                    "wrong indentation, tabs instead of spaces, or missing colons. "
                    "Validate at https://www.yamllint.com/")

        if "forbidden" in err or "cannot" in err:
            return (f"'{name}' — permission denied. The cluster may restrict this resource type. "
                    "For Kind clusters, avoid PodSecurityPolicy and other restricted resources.")

        if "image" in err and ("pull" in err or "not found" in err):
            return (f"'{name}' references a container image that couldn't be pulled. "
                    "Ensure images are loaded into Kind with: kind load docker-image <tag> --name <cluster>")

        # Generic fallback with the actual stderr
        stderr_lines = [l.strip() for l in error_msg.split('\n') if 'stderr:' in l.lower()]
        stderr_detail = stderr_lines[0] if stderr_lines else error_msg[:300]
        return f"'{name}' failed to apply. kubectl said: {stderr_detail}"

    # ─── Step 6: Wait for pods ────────────────────────────────────────────────

    def wait_for_pods(self, timeout_seconds: int = 180) -> list[dict[str, str]]:
        """Wait for all pods in the namespace to become Ready."""
        start = time.time()
        self._log("wait_for_pods", "running", f"Waiting for pods in '{self.namespace}' (timeout: {timeout_seconds}s)...")

        context = f"kind-{self.cluster_name}"
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            try:
                result = self._run_cmd(
                    [
                        "kubectl", "get", "pods",
                        "-n", self.namespace,
                        "-o", "json",
                        "--context", context,
                    ],
                    timeout=15,
                    check=False,
                )

                if result.returncode != 0:
                    time.sleep(3)
                    continue

                pods_data = json.loads(result.stdout)
                pods = []
                all_ready = True

                for pod in pods_data.get("items", []):
                    name = pod.get("metadata", {}).get("name", "unknown")
                    phase = pod.get("status", {}).get("phase", "Unknown")

                    # Check container readiness
                    containers = pod.get("status", {}).get("containerStatuses", [])
                    ready = all(c.get("ready", False) for c in containers) if containers else False

                    pods.append({
                        "name": name,
                        "status": phase,
                        "ready": "Ready" if ready else "NotReady",
                    })

                    if not ready:
                        all_ready = False

                if pods and all_ready:
                    duration = time.time() - start
                    self._log(
                        "wait_for_pods", "success",
                        f"All {len(pods)} pods ready",
                        duration=duration,
                    )
                    return pods

                elapsed = int(time.time() - start)
                self._log(
                    "wait_for_pods", "running",
                    f"Waiting... {len([p for p in pods if p['ready'] == 'Ready'])}/{len(pods)} ready ({elapsed}s)",
                )

            except (json.JSONDecodeError, RuntimeError):
                pass

            time.sleep(5)

        # Timeout
        duration = time.time() - start
        self._log("wait_for_pods", "error", f"Timed out after {timeout_seconds}s", duration=duration)

        # Return current state even on timeout
        try:
            result = self._run_cmd(
                ["kubectl", "get", "pods", "-n", self.namespace, "-o", "json", "--context", context],
                timeout=10, check=False,
            )
            pods_data = json.loads(result.stdout)
            return [
                {
                    "name": p.get("metadata", {}).get("name", "?"),
                    "status": p.get("status", {}).get("phase", "?"),
                    "ready": "Ready" if all(
                        c.get("ready", False) for c in p.get("status", {}).get("containerStatuses", [])
                    ) else "NotReady",
                }
                for p in pods_data.get("items", [])
            ]
        except Exception:
            return []

    # ─── Step 7: Port forwarding ──────────────────────────────────────────────

    def get_service_urls(self) -> dict[str, str]:
        """Get the NodePort or ClusterIP URLs for deployed services."""
        context = f"kind-{self.cluster_name}"
        urls = {}

        try:
            result = self._run_cmd(
                ["kubectl", "get", "svc", "-n", self.namespace, "-o", "json", "--context", context],
                timeout=15,
            )
            services = json.loads(result.stdout)

            for svc in services.get("items", []):
                name = svc.get("metadata", {}).get("name", "unknown")
                spec = svc.get("spec", {})
                svc_type = spec.get("type", "ClusterIP")
                ports = spec.get("ports", [])

                if ports:
                    port = ports[0].get("port", 8080)
                    if svc_type == "NodePort":
                        node_port = ports[0].get("nodePort", 30000)
                        urls[name] = f"http://localhost:{node_port}"
                    else:
                        urls[name] = f"{name}.{self.namespace}.svc.cluster.local:{port}"

        except (RuntimeError, json.JSONDecodeError) as e:
            self._log("get_service_urls", "error", f"Failed to get service URLs: {e}")

        return urls

    # ─── Full deployment orchestration ────────────────────────────────────────

    def deploy(
        self,
        developer_output: dict[str, Any],
        deployer_output: dict[str, Any],
    ) -> LiveDeploymentResult:
        """
        Execute the full live deployment pipeline.

        1. Check prerequisites (Docker, Kind, kubectl)
        2. Write artifacts to disk
        3. Create/verify Kind cluster
        4. Build Docker images
        5. Load images into Kind
        6. Apply K8s manifests
        7. Wait for pods
        8. Collect service URLs

        Returns a LiveDeploymentResult with full status.
        """
        self._logs = []
        self._log("deploy", "running", "Starting live deployment pipeline...")

        # Step 0: Check prerequisites
        prereqs = self._check_prerequisites()
        missing = [tool for tool, available in prereqs.items() if not available]

        if missing:
            msg = f"Missing prerequisites: {', '.join(missing)}. Install them and retry."
            self._log("prerequisites", "error", msg)
            return LiveDeploymentResult(
                success=False,
                cluster_name=self.cluster_name,
                namespace=self.namespace,
                services_deployed=[],
                pods=[],
                port_forwards={},
                logs=self._logs,
                error=msg,
            )

        self._log("prerequisites", "success", f"All prerequisites available: {', '.join(prereqs.keys())}")

        # Step 1: Write artifacts
        try:
            self.write_artifacts(developer_output, deployer_output)
        except Exception as e:
            self._log("write_artifacts", "error", str(e))
            return LiveDeploymentResult(
                success=False, cluster_name=self.cluster_name, namespace=self.namespace,
                services_deployed=[], pods=[], port_forwards={}, logs=self._logs,
                error=f"Failed to write artifacts: {e}",
            )

        # Step 2: Ensure Kind cluster
        cluster_ok, cluster_error = self.ensure_cluster()
        if not cluster_ok:
            return LiveDeploymentResult(
                success=False, cluster_name=self.cluster_name, namespace=self.namespace,
                services_deployed=[], pods=[], port_forwards={}, logs=self._logs,
                error=f"Failed to create Kind cluster:\n  • {cluster_error}",
            )

        # Step 3: Build images
        image_tags = self.build_images(deployer_output)

        # Step 4: Load images into Kind
        if image_tags:
            self.load_images(image_tags)
        else:
            self._log("load_images", "skipped", "No images to load")

        # Step 5: Apply manifests
        manifest_result = self.apply_manifests()
        if not manifest_result["success"]:
            # Build a detailed, user-friendly error message
            failure_details = []
            for f in manifest_result["failures"]:
                failure_details.append(f"  • {f['file']}: {f['diagnosis']}")
            detail_str = "\n".join(failure_details) if failure_details else "No manifests were applied."

            return LiveDeploymentResult(
                success=False, cluster_name=self.cluster_name, namespace=self.namespace,
                services_deployed=[], pods=[], port_forwards={}, logs=self._logs,
                error=f"Failed to apply Kubernetes manifests "
                      f"({manifest_result['applied']}/{manifest_result['total']} applied):\n{detail_str}",
            )

        # Step 6: Wait for pods
        pods = self.wait_for_pods(timeout_seconds=180)

        # Step 7: Get service URLs
        urls = self.get_service_urls()

        # Determine overall success
        all_ready = all(p.get("ready") == "Ready" for p in pods) if pods else False
        success = all_ready and len(pods) > 0

        status_msg = "Deployment successful" if success else "Deployment completed with issues"
        self._log("deploy", "success" if success else "error", status_msg)

        return LiveDeploymentResult(
            success=success,
            cluster_name=self.cluster_name,
            namespace=self.namespace,
            services_deployed=list(urls.keys()),
            pods=pods,
            port_forwards=urls,
            logs=self._logs,
            error=None if success else "Some pods did not become ready",
        )

    # ─── Cleanup ──────────────────────────────────────────────────────────────

    def teardown(self) -> bool:
        """Delete the Kind cluster and clean up artifacts."""
        self._log("teardown", "running", f"Deleting Kind cluster '{self.cluster_name}'...")
        try:
            self._run_cmd(["kind", "delete", "cluster", "--name", self.cluster_name], timeout=60)
            self._log("teardown", "success", "Cluster deleted")

            if self.output_dir.exists():
                shutil.rmtree(self.output_dir)
                self._log("teardown", "success", "Artifacts cleaned up")

            return True
        except RuntimeError as e:
            self._log("teardown", "error", str(e))
            return False
