import { isMainThread, workerData, parentPort } from 'worker_threads';
import { FirestoreBackupReader } from './firestore';

if (isMainThread) {
  console.log('workerThread should not be main thread');
  process.exit(0);
}

function post(data: any) {
  parentPort?.postMessage(data);
}

async function run() {
  const fullpath: string = workerData.fullpath;
  const reader = new FirestoreBackupReader(fullpath, true);
  await reader.readAll(post);
}

run();
