/**
 * @template T
 * @typedef {function(): Promise<T>} IO<T>
 */

/**
 * @template T
 * @param {T} t
 * @returns {IO<T>}
 */
export function of(t) {
  return () => {
    return Promise.resolve(t);
  };
}

/**
 * @template T
 * @template U
 * @param {IO<T>} io
 * @param {function(T): IO<U>} mapper
 * @returns {IO<U>}
 */
export function bind(io, mapper) {
  return () => {
    return Promise.resolve()
      .then((_) => io())
      .then((t) => mapper(t)());
  };
}

/**
 * @template T
 * @template U
 * @param { function(T): Promise<U> } pure
 * @returns { function(T): IO<U> }
 */
export function lift(pure) {
  return (t) => () => pure(t);
}

/**
 * @template K
 * @template V
 * @param { IO<K[]> } ikeys
 * @param { IO<V[]> } ivals
 * @returns { IO<Map<K, V>> }
 */
export function kvpairs2map(ikeys, ivals) {
  return bind(
    ikeys,
    (keys) => {
      return bind(
        ivals,
        lift((vals) => {
          const gpairs = function* () {
            /** @type number */
            const ksz = keys.length;

            for (let i = 0; i < ksz; i++) {
              /** @type K */
              const key = keys[i];

              /** @type V? */
              const oval = vals[i] ?? null;

              if (!oval) return;

              /** @type V */
              const val = oval;

              yield { key, val };
            }
          };

          const ipairs = Array.from(gpairs());

          /** @type Array<[K, V]> */
          const mapd = ipairs.map((pair) => {
            const { key, val } = pair;
            return [key, val];
          });

          return Promise.resolve(new Map(mapd));
        }),
      );
    },
  );
}
