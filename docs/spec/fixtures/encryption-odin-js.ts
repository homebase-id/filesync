// Golden vectors for hbsync's Homebase encryption, made with odin-js's own functions.
// Run from an odin-js checkout: npx vite-node ../filesync/docs/spec/fixtures/encryption-odin-js.ts > ../filesync/docs/spec/fixtures/encryption-odin-js.json
const L = `${process.cwd()}/packages/libs/js-lib/src`;
const { encryptKeyHeader, encryptWithKeyheader, decryptChunkedBytesResponse } = await import(`${L}/core/DriveData/SecurityHelpers`);
const { encryptMetaData } = await import(`${L}/core/DriveData/Upload/UploadHelpers`);
const { getRangeHeader } = await import(`${L}/core/DriveData/File/DriveFileHelper`);

const seq = (from: number, n = 16) => new Uint8Array(Array.from({ length: n }, (_, i) => (from + i) & 0xff));
const hex = (b: Uint8Array) => Buffer.from(b).toString('hex');
const b64 = (b: Uint8Array) => Buffer.from(b).toString('base64');

const sharedSecret = seq(0x10);
const key = seq(0x20);
const headerIv = seq(0x30);
const transferIv = seq(0x40);
const payloadIv = seq(0x50);
const payload = new Uint8Array(Array.from({ length: 100 }, (_, i) => (i * 7) & 0xff));
const content = '{"v":1,"name":"Résumé.txt","mtime":1759920000123,"size":100}';
const client = { getSharedSecret: () => sharedSecret };

const ekh = await encryptKeyHeader(client, { iv: headerIv, aesKey: key }, transferIv);
const sealedPayload: Uint8Array = await encryptWithKeyheader(payload, { iv: payloadIv, aesKey: key });
const meta = await encryptMetaData({ appData: { content } }, { iv: headerIv, aesKey: key });

// A ranged read as odin-js does it: the key header of the payload response, then the CBC trick.
const pkh = await encryptKeyHeader(client, { iv: payloadIv, aesKey: key }, transferIv);
const version = new Uint8Array([1, 0, 0, 0]);
const header64 = b64(new Uint8Array([...pkh.iv, ...pkh.encryptedAesKey, ...version]));
const [chunkStart, chunkEnd] = [40, 80];
const { startOffset, updatedChunkStart, updatedChunkEnd } = getRangeHeader(chunkStart, chunkEnd);
const fetched = sealedPayload.slice(updatedChunkStart, updatedChunkEnd + 1);
const response = { data: fetched.buffer.slice(fetched.byteOffset, fetched.byteOffset + fetched.byteLength), headers: { payloadencrypted: 'True', sharedsecretencryptedheader64: header64 } };
const range: Uint8Array = await decryptChunkedBytesResponse(client, response, startOffset, updatedChunkStart);

console.log(JSON.stringify({
  _comment: 'Made with odin-js js-lib (SecurityHelpers.encryptKeyHeader, encryptWithKeyheader, decryptChunkedBytesResponse; UploadHelpers.encryptMetaData; DriveFileHelper.getRangeHeader) by docs/spec/fixtures/encryption-odin-js.ts. Bytes are hex unless named base64.',
  sharedSecret: hex(sharedSecret), key: hex(key), headerIv: hex(headerIv), transferIv: hex(transferIv), payloadIv: hex(payloadIv),
  encryptedAesKey: hex(ekh.encryptedAesKey),
  content, sealedContentBase64: meta.appData.content,
  payload: hex(payload), sealedPayload: hex(sealedPayload),
  payloadKeyHeader64: header64,
  range: { start: chunkStart, end: chunkEnd, fetchStart: updatedChunkStart, fetchEndInclusive: updatedChunkEnd, plain: hex(range.slice(0, chunkEnd - chunkStart)) },
}, null, 2));
