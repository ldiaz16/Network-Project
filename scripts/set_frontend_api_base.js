#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

function readArgValue(args, name) {
  const exactIndex = args.indexOf(name);
  if (exactIndex !== -1) {
    if (exactIndex === args.length - 1) {
      throw new Error(`Missing value for ${name}`);
    }
    return args[exactIndex + 1];
  }

  const prefix = `${name}=`;
  const prefixed = args.find((arg) => arg.startsWith(prefix));
  if (prefixed) {
    return prefixed.slice(prefix.length);
  }

  return undefined;
}

function updateApiBase(htmlPath, apiBase) {
  const metaRegex = /(<meta\s+name="api-base"\s+content=")([^"]*)(")/i;
  const html = fs.readFileSync(htmlPath, "utf8");
  const match = html.match(metaRegex);
  if (!match) {
    return false;
  }
  const updatedHtml = html.replace(metaRegex, (_match, before, _content, after) => `${before}${apiBase}${after}`);
  fs.writeFileSync(htmlPath, updatedHtml, "utf8");
  return true;
}

function main() {
  const args = process.argv.slice(2);
  const apiBaseArg = readArgValue(args, "--api-base");
  const indexArg = readArgValue(args, "--index");
  const frontendDirArg = readArgValue(args, "--frontend-dir") ?? "frontend";

  const rawApiBase = apiBaseArg ?? process.env.API_BASE_URL;
  if (!rawApiBase) {
    throw new Error("API base URL not provided. Pass --api-base or set API_BASE_URL.");
  }

  const apiBase = rawApiBase.replace(/\/+$/, "");

  let htmlPaths = [];
  if (indexArg) {
    const resolved = path.resolve(indexArg);
    if (!fs.existsSync(resolved)) {
      throw new Error(`Cannot find index file at ${resolved}`);
    }
    htmlPaths = [resolved];
  } else {
    const frontendDir = path.resolve(frontendDirArg);
    if (!fs.existsSync(frontendDir) || !fs.statSync(frontendDir).isDirectory()) {
      throw new Error(`Cannot find frontend directory at ${frontendDir}`);
    }
    htmlPaths = fs
      .readdirSync(frontendDir)
      .filter((name) => name.toLowerCase().endsWith(".html"))
      .sort()
      .map((name) => path.join(frontendDir, name));

    if (htmlPaths.length === 0) {
      throw new Error(`No HTML files found under ${frontendDir}`);
    }
  }

  let updated = 0;
  for (const htmlPath of htmlPaths) {
    if (updateApiBase(htmlPath, apiBase)) {
      updated += 1;
    }
  }

  if (updated === 0) {
    throw new Error('Failed to locate <meta name="api-base"> tag in the provided HTML file(s).');
  }

  if (htmlPaths.length === 1) {
    process.stdout.write(`Updated API base to ${apiBase}\n`);
  } else {
    process.stdout.write(`Updated API base to ${apiBase} in ${updated} file(s)\n`);
  }
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.message ?? String(error)}\n`);
  process.exit(1);
}

