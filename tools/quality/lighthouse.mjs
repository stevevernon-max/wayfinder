import { mkdirSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const target = process.argv[2] ?? "http://127.0.0.1:8765/";
const lighthouseCli = fileURLToPath(
  new URL("./node_modules/lighthouse/cli/index.js", import.meta.url),
);
mkdirSync("reports", { recursive: true });

for (const device of ["mobile", "desktop"]) {
  const args = [
    lighthouseCli,
    target,
    "--quiet",
    "--only-categories=performance,best-practices",
    "--output=json",
    "--output=html",
    `--output-path=reports/${device}`,
    "--chrome-flags=--headless",
  ];
  if (new URL(target).hostname === "127.0.0.1") {
    args.push("--blocked-url-patterns=https://*");
  }
  if (device === "desktop") args.push("--preset=desktop");
  const result = spawnSync(process.execPath, args, { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
