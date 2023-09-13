import {
  readOneFirestoreExport,
  readAllFirestoreExport,
  FirestoreBackupReader,
  readAllFirestoreExportThreads,
} from './firestore';

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

async function test0() {
  let time = Date.now();
  let i = 0;
  readAllFirestoreExport(
    '/mnt/data/firestorebackup/IG-2023-08-22T20:00:05_99844/all_namespaces/kind_v1_IGUser/all_namespaces_kind_v1_IGUser.export_metadata',
    (data: any) => {
      i++;
      if (i % 10000 === 0) {
        console.log(i, Date.now() - time);
      }
    }
  );
}

async function test1() {
  let time = Date.now();
  let i = 0;
  await readAllFirestoreExportThreads(
    '/mnt/data/firestorebackup/IG-2023-08-22T20:00:05_99844/all_namespaces/kind_v1_IGUser/all_namespaces_kind_v1_IGUser.export_metadata',
    (data: any) => {
      i++;
      if (i % 10000 === 0) {
        console.log(i, Date.now() - time);
      }
    },
    6
  );
}

test1();

// readOneOutput();
// readOneOutput2();
// readAllOutput();
