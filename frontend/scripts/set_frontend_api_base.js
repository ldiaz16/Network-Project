#!/usr/bin/env node
const { spawnSync } = require("node:child_process");
const path = require("node:path");

const args = process.argv.slice(2);

// Run the existing python script (wherever it lives)
const scriptPath = path.join(process.cwd(), "scripts", "set_frontend_api_base.py");

const result = spawnSync("python3", [scriptPath, ...args], {
  stdio: "inherit",
});

process.exit(result.status ?? 1);
