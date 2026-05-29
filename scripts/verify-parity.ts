import matrix from "../docs/parity/matrix.json";
import upstream from "../docs/parity/upstream.json";

const deferredRows = matrix.rows.filter((row) => row.status === "deferred");

console.log(`Parity reference: ${upstream.packageName} ${upstream.version}`);
console.log(`Pinned commit: ${upstream.commit}`);
console.log(`Submodule path: ${upstream.submodulePath}`);
console.log(`Matrix rows: ${matrix.rows.length}`);
console.log(`Deferred rows: ${deferredRows.length}`);
console.log("");
console.log("Optional upstream check when the Python environment is available:");
console.log(`cd ${upstream.submodulePath} && make test`);
