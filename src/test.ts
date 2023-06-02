import { readOneFirestoreExport, readAllFirestoreExport, FirestoreBackupReader } from './firestore';

async function readOneOutput() {
  console.time('readOneOutput');
  await readOneFirestoreExport(
    '/mnt/data/firestorebackup/2023-03-13T18:00:03_70712/all_namespaces/kind_v1_Channel/output-0',
    (data: any) => {
      // console.log(data);
    }
  );
  // 268M 9.3s
  console.timeEnd('readOneOutput');
}

async function readOneOutput2() {
  console.time('readOneOutput2');
  const reader = new FirestoreBackupReader(
    '/mnt/data/firestorebackup/2023-03-13T18:00:03_70712/all_namespaces/kind_v1_Channel/output-0',
    true
  );
  while (!reader.isEnd) {
    const v = await reader.readOne();
  }
  console.timeEnd('readOneOutput2');
}

async function readAllOutput() {
  console.time('readAllOutput');
  await readAllFirestoreExport(
    '/mnt/data/firestorebackup/2023-03-13T18:00:03_70712/all_namespaces/kind_v1_Channel/all_namespaces_kind_v1_Channel.export_metadata',
    (data: any) => {
      // console.log(data);
    }
  );
  console.timeEnd('readAllOutput');
}

// readOneOutput();
// readOneOutput2();
// readAllOutput();
