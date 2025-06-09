import { bind, lift } from "./io.mjs";

/** @import { IO } from "./io.mjs" */

/**
 * @readonly
 * @enum {number}
 */
export const FilterResult = Object.freeze({
  UNKNOWN: 0,
  NOT_FOUND: 1,
  MAY_EXIST: 2,
  FOUND: 3,
});

/** @typedef {number} BucketSerial */

/** @typedef {number} TinyBloom16bits */

/**
 * @typedef {object} BucketBloom16
 * @property {BucketSerial} serial
 * @property {TinyBloom16bits} bloomData
 */

/**
 * @template H
 * @template F
 * @param {F} filterInfo
 * @param {function(F): IO<H>} filter2hash
 * @param {Array<BucketBloom16>} tinyBloomValues
 * @param {function(H,TinyBloom16bits): FilterResult} hash2result
 * @returns {IO<Array<BucketSerial>>}
 */
export function filterSerials(
  filterInfo,
  filter2hash,
  tinyBloomValues,
  hash2result,
) {
  /** @type IO<H> */
  const ihash = filter2hash(filterInfo);

  return bind(
    ihash,
    lift((hash) => {
      /** @type Array<BucketSerial> */
      const serials = tinyBloomValues.flatMap((bpair) => {
        /** @type BucketSerial */
        const serial = bpair.serial;

        /** @type TinyBloom16bits */
        const bloomData = bpair.bloomData;

        /** @type FilterResult */
        const result = hash2result(hash, bloomData);

        return FilterResult.MAY_EXIST === result ? [serial] : [];
      });
      return Promise.resolve(serials);
    }),
  );
}

/**
 * @template H
 * @template F
 * @template I
 * @template B
 * @param {F} filterInfo
 * @param {function(F): IO<H>} filter2hash
 * @param {Array<BucketBloom16>} tinyBloomValues
 * @param {function(H,TinyBloom16bits): FilterResult} hash2result
 * @param {function(BucketSerial): IO<B>} bucketSource
 * @param {function(B): IO<Array<I>>} bucket2items
 * @param {function(F, I): FilterResult} filterItem
 * @param {number} maxNumberOfBuckets
 * @returns {IO<Array<I>>}
 */
export function getItems(
  filterInfo,
  filter2hash,
  tinyBloomValues,
  hash2result,
  bucketSource,
  bucket2items,
  filterItem,
  maxNumberOfBuckets = 256,
) {
  /** @type IO<Array<BucketSerial>> */
  const iserials = filterSerials(
    filterInfo,
    filter2hash,
    tinyBloomValues,
    hash2result,
  );

  /** @type IO<Array<B>> */
  const ibuckets = bind(
    iserials,
    (serials) => {
      /** @type Array<BucketSerial> */
      const taken = serials.slice(0, maxNumberOfBuckets);

      /** @type Array<IO<B>> */
      const ios = taken.map(bucketSource);

      return () => Promise.all(ios.map((i) => i()));
    },
  );

  return bind(
    ibuckets,
    (buckets) => {
      /** @type Array<IO<I[]>> */
      const ios = buckets.map(bucket2items);

      return () => {
        /** @type Promise<Array<I[]>> */
        const parr = Promise.all(ios.map((i) => i()));

        return parr.then((arr) =>
          arr.flatMap((items) => {
            return items.filter((item) => {
              /** @type FilterResult */
              const result = filterItem(filterInfo, item);
              return FilterResult.FOUND === result;
            });
          })
        );
      };
    },
  );
}

/**
 * @param {number} hash4x4bits
 * @param {TinyBloom16bits} bloom16bits
 * @returns {FilterResult}
 */
export function hash2result16bits(hash4x4bits, bloom16bits) {
  const h0 = hash4x4bits >> 12;
  const h1 = hash4x4bits >> 8;
  const h2 = hash4x4bits >> 4;
  const h3 = hash4x4bits;

  const p0 = 1 << (h0 & 0x0f);
  const p1 = 1 << (h1 & 0x0f);
  const p2 = 1 << (h2 & 0x0f);
  const p3 = 1 << (h3 & 0x0f);

  const a0 = p0 & bloom16bits;
  const a1 = p1 & bloom16bits;
  const a2 = p2 & bloom16bits;
  const a3 = p3 & bloom16bits;

  const notFound0 = 0 === a0;
  const notFound1 = 0 === a1;
  const notFound2 = 0 === a2;
  const notFound3 = 0 === a3;

  /** @type boolean */
  const notFound = [
    notFound0,
    notFound1,
    notFound2,
    notFound3,
  ].some((b) => b);

  return notFound ? FilterResult.NOT_FOUND : FilterResult.MAY_EXIST;
}

/**
 * @param {BigInt} bi
 * @param {DataView} view2array
 */
export function bigint2bufferBE(bi, view2array) {
  view2array.setBigInt64(0, bi, false);
}

/** @type function(ArrayBuffer): number */
export function buf32tohash4x4bits(buf32) {
  const dv256 = new DataView(buf32);

  const bu0 = dv256.getBigUint64(0, false);
  const bu1 = dv256.getBigUint64(8, false);
  const bu2 = dv256.getBigUint64(16, false);
  const bu3 = dv256.getBigUint64(24, false);

  const bue = bu0 ^ bu2;
  const buo = bu1 ^ bu3;

  const bueo = bue ^ buo;

  const hi = bueo >> 32n;
  const lo = bueo & 0xffff_ffffn;

  const hl = hi ^ lo; // 32-bit

  const h = hl >> 16n;
  const l = hl & 0xffffn;

  return Number(h ^ l);
}

/**
 * @param {BigInt} bi
 * @param {DataView} view2array
 * @returns {IO<number>} The computed hash(4x 4-bits = 16-bits)
 */
export function bigint2hash4x4bits(bi, view2array) {
  return () => {
    bigint2bufferBE(bi, view2array);

    /** @type Promise<ArrayBuffer> */
    const ph256 = crypto.subtle.digest("SHA-256", view2array);

    return ph256.then(buf32tohash4x4bits);
  };
}

/**
 * @param {string} ascii256
 * @param {TextEncoder} str2ascii
 * @param {Uint8Array} buf256
 * @returns {IO<number>} The computed hash(4x 4-bits = 16-bits)
 */
export function ascii2hash4x4bits256(ascii256, str2ascii, buf256) {
  return () => {
    /** @type number */
    const written = str2ascii.encodeInto(ascii256, buf256).written;

    /** @type Uint8Array */
    const taken = buf256.slice(0, written);

    /** @type Promise<ArrayBuffer> */
    const p256 = crypto.subtle.digest("SHA-256", taken);

    return p256.then(buf32tohash4x4bits);
  };
}
