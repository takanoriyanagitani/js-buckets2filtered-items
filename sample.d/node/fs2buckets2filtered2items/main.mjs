import {
  bigint2hash4x4bits,
  FilterResult,
  getItems,
  hash2result16bits,
} from "./index.mjs";

import { bind } from "./io.mjs";

/** @import { IO } from "./io.mjs" */

/** @import { BucketBloom16, BucketSerial } from "./index.mjs" */

/**
 * @typedef {object} SampleFilterInfo
 * @property {BigInt} userId
 */

/** @type SampleFilterInfo */
const filterInfo = Object.freeze({
  userId: 3776n,
});

/** @type BigInt64Array */
const buf4hash = new BigInt64Array(1);

/** @type DataView */
const view4hash = new DataView(buf4hash.buffer);

/** @type function(SampleFilterInfo): IO<number> */
const filter2hash = function (finfo) {
  return bigint2hash4x4bits(finfo.userId, view4hash);
};

/** @type Array<BucketBloom16> */
const tinyBloomValues = [
  Object.freeze({ serial: 0x42, bloomData: 0x1402 }),
  Object.freeze({ serial: 0x43, bloomData: 0x1402 }),
];

/** @type function(number, number): FilterResult */
const hash2result = function (hash4x4bits, bloom16bits) {
  return hash2result16bits(hash4x4bits, bloom16bits);
};

/**
 * @typedef {object} SampleItem
 * @property {BigInt} userId
 * @property {number} orderId
 * @property {number} unixtimeMs
 */

/**
 * @typedef {object} SampleBucket
 * @property {number} bucketNumber
 * @property {Array<SampleItem>} items
 */

/** @type Map<BucketSerial, SampleBucket> */
const buckets = new Map([
  Object.freeze({
    bucketNumber: 0x42,
    items: [
      Object.freeze({
        userId: 3776n,
        orderId: 333,
        unixtimeMs: 1749430561879,
      }),
      Object.freeze({
        userId: 599n,
        orderId: 334,
        unixtimeMs: 1749430562879,
      }),
    ],
  }),
  Object.freeze({
    bucketNumber: 0x43,
    items: [
      Object.freeze({
        userId: 599n,
        orderId: 634,
        unixtimeMs: 1749430563879,
      }),
      Object.freeze({
        userId: 599n,
        orderId: 635,
        unixtimeMs: 1749430564879,
      }),
    ],
  }),
].map((bucket) => [
  bucket.bucketNumber,
  bucket,
]));

/** @type function(BucketSerial): IO<SampleBucket> */
const bucketSource = function (bno) {
  return () => {
    /** @type SampleBucket? */
    const obucket = buckets.get(bno) ?? null;

    if (!obucket) return Promise.reject(new Error(`bucket ${bno} not found`));

    console.debug(`candidate bucket number: ${bno}`);
    /** @type SampleBucket */
    const bkt = obucket;

    return Promise.resolve(bkt);
  };
};

/** @type function(SampleBucket): IO<Array<SampleItem>> */
const bucket2items = function (bkt) {
  return () => Promise.resolve(bkt.items);
};

/** @type function(SampleFilterInfo, SampleItem): FilterResult */
const filterItem = function (finfo, item) {
  return finfo.userId === item.userId
    ? FilterResult.FOUND
    : FilterResult.NOT_FOUND;
};

/** @type function(Array<SampleItem>): IO<Void> */
const printItems = (items) => {
  return () => {
    console.info(`number of items: ${items.length}`);
    for (const item of items) {
      console.info(item);
    }
    return Promise.resolve();
  };
};

/** @type IO<Void> */
const main = () => {
  /** @type IO<Array<SampleItem>> */
  const iitems = getItems(
    filterInfo,
    filter2hash,
    tinyBloomValues,
    hash2result,
    bucketSource,
    bucket2items,
    filterItem,
  );

  return bind(iitems, printItems)();
};

main().catch(console.error);
