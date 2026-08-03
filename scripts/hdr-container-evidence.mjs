import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const policyPath = resolve(root, "workers/hdr/container-supply-chain-policy.json");
const policy = JSON.parse(readFileSync(policyPath, "utf8"));
const mode = process.argv[2] ?? "--preflight";
if (!["--preflight", "--build"].includes(mode)) throw new Error("Usage: hdr-container-evidence.mjs --preflight|--build");

const commandAvailable = (command) => spawnSync(command, ["--version"], { encoding: "utf8", stdio: "ignore" }).status === 0;
const baseImage = process.env.HDR_BASE_IMAGE ?? "";
const lockPath = resolve(root, policy.pythonLockFile);
const dockerfile = readFileSync(resolve(root, "workers/hdr/Dockerfile"), "utf8");
const blockers = [];

if (!/^.+@sha256:[a-f0-9]{64}$/.test(baseImage) || !policy.approvedBaseImages.includes(baseImage)) {
  blockers.push("approved_base_image_missing");
}
if (!existsSync(lockPath)) {
  blockers.push("hashed_python_lock_missing");
} else {
  const lock = readFileSync(lockPath, "utf8");
  const requirements = lock.split(/\n(?=[A-Za-z0-9_.-]+==)/).filter((line) => line.trim() && !line.trimStart().startsWith("#"));
  if (requirements.length === 0 || requirements.some((line) => !/^[A-Za-z0-9_.-]+==[^\s\\]+/.test(line.trim()) || !/--hash=sha256:[a-f0-9]{64}/.test(line))) {
    blockers.push("python_lock_not_fully_hashed");
  }
}
if (!dockerfile.includes("requirements.lock") || !dockerfile.includes("--require-hashes")) {
  blockers.push("dockerfile_hash_lock_not_enforced");
}
for (const command of ["docker", "syft", "grype"]) {
  if (!commandAvailable(command)) blockers.push(`${command}_unavailable`);
}

const preflight = {
  schemaVersion: "hdr-container-evidence.v1",
  status: blockers.length === 0 ? "ready_for_local_build" : "blocked",
  baseImageApproved: blockers.indexOf("approved_base_image_missing") === -1,
  hashedLockVerified: blockers.every((item) => item !== "hashed_python_lock_missing" && item !== "python_lock_not_fully_hashed"),
  blockers,
};

if (mode === "--preflight" || blockers.length > 0) {
  process.stdout.write(`${JSON.stringify(preflight, null, 2)}\n`);
  process.exit(blockers.length === 0 ? 0 : 2);
}

const evidenceDirectory = resolve(root, ".artifacts/hdr-container", new Date().toISOString().replace(/[:.]/g, "-"));
mkdirSync(evidenceDirectory, { recursive: true });
writeFileSync(resolve(evidenceDirectory, "preflight.json"), `${JSON.stringify(preflight, null, 2)}\n`);

function run(command, args, outputFile) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  if (result.status !== 0) throw new Error(`${command} failed with exit ${result.status ?? "unknown"}`);
  if (outputFile) writeFileSync(resolve(evidenceDirectory, outputFile), result.stdout);
}

const tag = policy.imageTag;
run("docker", ["build", "--file", "workers/hdr/Dockerfile", "--build-arg", `HDR_BASE_IMAGE=${baseImage}`, "--tag", tag, "."]);
run("docker", ["image", "inspect", tag], "image-inspect.json");
run("syft", [tag, "-o", "spdx-json"], "sbom.spdx.json");
run("grype", [tag, "-o", "json", "--fail-on", "high", "--config", ".grype.yaml"], "vulnerability-report.json");
process.stdout.write(`${JSON.stringify({ ...preflight, status: "local_build_evidence_created", evidenceDirectory }, null, 2)}\n`);
