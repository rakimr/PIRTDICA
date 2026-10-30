import { readFileSync } from "node:fs";
import { ReplitConnectors } from "@replit/connectors-sdk";

const request = JSON.parse(readFileSync(0, "utf8"));
const connectors = new ReplitConnectors();

try {
  const headers = {
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
  };
  const options = {
    method: request.method || "GET",
    headers,
  };

  if (request.data !== undefined && request.data !== null) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(request.data);
  }

  const response = await connectors.proxy("github", request.path, options);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { message: text.slice(0, 500) };
  }

  process.stdout.write(JSON.stringify({
    status: response.status,
    body,
  }));
} catch (error) {
  // Keep any connector diagnostics free of request credentials.
  process.stderr.write(`GitHub connector request failed: ${error?.name || "Error"}\n`);
  process.exitCode = 1;
}
