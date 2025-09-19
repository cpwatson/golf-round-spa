#!/usr/bin/env node
'use strict';

const {promises: fsPromises} = require('fs');
const path = require('path');
const {Storage} = require('@google-cloud/storage');

function printHelp() {
  console.log(`\nUpload a JSON document to a Google Cloud Storage bucket.\n\n` +
    `Usage: npm run upload-json -- --bucket <bucket-name> [options]\n\n` +
    `Options:\n` +
    `  --bucket, -b           GCS bucket name (required)\n` +
    `  --destination, -d      Destination object path in the bucket. Defaults to the input file name\n` +
    `                         or document-<timestamp>.json when reading from stdin.\n` +
    `  --input, -i            Path to a JSON file to upload.\n` +
    `  --stdin                Read JSON from standard input (also implied when data is piped).\n` +
    `  --data                 Inline JSON string to upload.\n` +
    `  --project, -p          GCP project id (optional).\n` +
    `  --credentials          Path to a service account key file (optional).\n` +
    `  --cache-control        Cache-Control metadata to apply to the uploaded object.\n` +
    `  --compact              Upload compact JSON instead of pretty-printing with two spaces.\n` +
    `  --dry-run              Validate input and report the upload target without performing it.\n` +
    `  --help, -h             Show this message.\n`);
}

function normalizeKey(rawKey) {
  const key = rawKey.toLowerCase();
  const map = {
    b: 'bucket',
    bucket: 'bucket',
    d: 'destination',
    dest: 'destination',
    destination: 'destination',
    i: 'input',
    input: 'input',
    p: 'project',
    project: 'project',
    credentials: 'credentials',
    'key-file': 'credentials',
    keyfile: 'credentials',
    data: 'data',
    stdin: 'stdin',
    compact: 'compact',
    'cache-control': 'cacheControl',
    cachecontrol: 'cacheControl',
    'dry-run': 'dryRun',
    dryrun: 'dryRun',
    h: 'help',
    help: 'help'
  };
  return map[key];
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    let token = argv[i];
    if (!token.startsWith('-')) {
      continue;
    }
    if (!token.startsWith('--')) {
      token = `--${token.slice(1)}`;
    }
    const withoutDashes = token.slice(2);
    let key = withoutDashes;
    let value;
    const eqIndex = withoutDashes.indexOf('=');
    if (eqIndex !== -1) {
      key = withoutDashes.slice(0, eqIndex);
      value = withoutDashes.slice(eqIndex + 1);
    }
    key = normalizeKey(key.replace(/^-+/, ''));
    if (!key) {
      continue;
    }
    if (value === undefined) {
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith('-')) {
        value = next;
        i += 1;
      } else {
        value = true;
      }
    }
    args[key] = value;
  }
  return args;
}

async function readFromStdin() {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', chunk => {
      data += chunk;
    });
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

async function getInput(options) {
  if (options.data) {
    return String(options.data);
  }
  if (options.input) {
    return fsPromises.readFile(options.input, 'utf8');
  }
  if (options.stdin || !process.stdin.isTTY) {
    const stdinData = await readFromStdin();
    if (!stdinData) {
      throw new Error('No data received from stdin.');
    }
    return stdinData;
  }
  throw new Error('No JSON input provided. Use --input, --data, or pipe data via stdin.');
}

function inferDestination(options) {
  if (options.destination) {
    return options.destination;
  }
  if (options.input) {
    return path.basename(options.input);
  }
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  return `document-${timestamp}.json`;
}

function preparePayload(rawJson, options) {
  let parsed;
  try {
    parsed = JSON.parse(rawJson);
  } catch (error) {
    throw new Error(`Input is not valid JSON: ${error.message}`);
  }
  const spaces = options.compact ? 0 : 2;
  const jsonString = JSON.stringify(parsed, null, spaces);
  return {
    jsonString,
    bytes: Buffer.byteLength(jsonString, 'utf8')
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }
  if (!args.bucket) {
    printHelp();
    throw new Error('Missing required --bucket option.');
  }
  const rawJson = await getInput(args);
  const payload = preparePayload(rawJson, args);
  const destination = inferDestination(args);
  const storage = new Storage({
    projectId: args.project,
    keyFilename: args.credentials
  });
  if (args.dryRun) {
    console.log(`[dry-run] Valid JSON (${payload.bytes} bytes). Would upload to gs://${args.bucket}/${destination}`);
    return;
  }
  const saveOptions = {
    resumable: false,
    contentType: 'application/json'
  };
  if (args.cacheControl) {
    saveOptions.metadata = {cacheControl: args.cacheControl};
  }
  await storage
    .bucket(args.bucket)
    .file(destination)
    .save(payload.jsonString, saveOptions);
  console.log(`Uploaded ${payload.bytes} bytes to gs://${args.bucket}/${destination}`);
}

main().catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
