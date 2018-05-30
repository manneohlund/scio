/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.values

import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.Implicits._

import java.lang.{Iterable => JIterable, Long => JLong}
import java.util.{Map => JMap}

import com.spotify.scio.ScioContext
import com.spotify.scio.util._
import com.spotify.scio.util.random.{BernoulliValueSampler, PoissonValueSampler}
import com.twitter.algebird._
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, PCollection, PCollectionView}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.reflect.ClassTag

private object PairSCollectionFunctions {

  private val logger = LoggerFactory.getLogger(this.getClass)

  case class BFSettings(width: Int, capacity: Int, numBFs: Int)

  def optimalBFSettings(numEntries: Long, fpProb: Double): BFSettings = {
    // double to int rounding error happens when numEntries > (1 << 27)
    // set numEntries upper bound to 1 << 27 to avoid high false positive
    def estimateWidth(numEntries: Int, fpProb: Double): Int =
      math.ceil(-1 * numEntries * math.log(fpProb) / math.log(2) / math.log(2)).toInt

    // upper bound of n as 2^x
    def upper(n: Int): Int = 1 << (0 to 27).find(1 << _ >= n).get

    // cap capacity between [minSize, maxSize] and find upper bound of 2^x
    val (minSize, maxSize) = (2048, 1 << 27)
    var capacity = upper(math.max(math.min(numEntries, maxSize).toInt, minSize))

    // find a width with the given capacity
    var width = estimateWidth(capacity, fpProb)
    while (width == Int.MaxValue) {
      capacity = capacity >> 1
      width = estimateWidth(capacity, fpProb)
    }
    val numBFs = (numEntries / capacity).toInt + 1

    val totalBytes = width.toLong * numBFs / 8
    logger.info(s"Optimal Bloom Filter settings for numEntries = $numEntries, fpProb = $fpProb")
    logger.info(
      s"BF width = $width, capacity = $capacity, numBFs = $numBFs, total bytes = $totalBytes")
    BFSettings(width, capacity, numBFs)
  }

}

// scalastyle:off number.of.methods
/**
 * Extra functions available on SCollections of (key, value) pairs through an implicit conversion.
 *
 * @groupname cogroup CoGroup Operations
 * @groupname join Join Operations
 * @groupname per_key Per Key Aggregations
 * @groupname transform Transformations
 */
class PairSCollectionFunctions[K: Coder, V: Coder](val self: SCollection[(K, V)]) {

  import TupleFunctions._

  private val context: ScioContext = self.context

  private val toKvTransform = ParDo.of(Functions.mapFn[(K, V), KV[K, V]](kv => KV.of(kv._1, kv._2)))

  private[scio] def toKV: SCollection[KV[K, V]] = {
    val coder = kvCoder[K, V]
    val o = self.applyInternal(toKvTransform).setCoder(coder)
    context.wrap(o)
  }

  private[values] def applyPerKey[UI, UO: Coder]
  (t: PTransform[PCollection[KV[K, V]], PCollection[KV[K, UI]]], f: KV[K, UI] => (K, UO))
  : SCollection[(K, UO)] = {
    val o = self.applyInternal(new PTransform[PCollection[(K, V)], PCollection[(K, UO)]](null) {
      override def expand(input: PCollection[(K, V)]): PCollection[(K, UO)] =
        input
          .apply("TupleToKv", toKvTransform)
          .setCoder(kvCoder[K, V])
          .apply(t)
          .apply("KvToTuple", ParDo.of(Functions.mapFn[KV[K, UI], (K, UO)](f)))
          .setCoder(Coder[(K, UO)])
    })
    context.wrap(o)
  }

  /**
   * Convert this SCollection to an [[SCollectionWithHotKeyFanout]] that uses an intermediate node
   * to combine "hot" keys partially before performing the full combine.
   * @param hotKeyFanout a function from keys to an integer N, where the key will be spread among
   * N intermediate nodes for partial combining. If N is less than or equal to 1, this key will
   * not be sent through an intermediate node.
   */
  def withHotKeyFanout(hotKeyFanout: K => Int): SCollectionWithHotKeyFanout[K, V] =
    new SCollectionWithHotKeyFanout(this, Left(hotKeyFanout))

  /**
   * Convert this SCollection to an [[SCollectionWithHotKeyFanout]] that uses an intermediate node
   * to combine "hot" keys partially before performing the full combine.
   * @param hotKeyFanout constant value for every key
   */
  def withHotKeyFanout(hotKeyFanout: Int): SCollectionWithHotKeyFanout[K, V] =
    new SCollectionWithHotKeyFanout(this, Right(hotKeyFanout))

  // =======================================================================
  // CoGroups
  // =======================================================================

  /**
   * For each key k in `this` or `that`, return a resulting SCollection that contains a tuple with
   * the list of values for that key in `this` as well as `that`.
   * @group cogroup
   */
  def cogroup[W: Coder](that: SCollection[(K, W)])
  : SCollection[(K, (Iterable[V], Iterable[W]))] =
    MultiJoin.withName(self.tfName).cogroup(self, that)

  /**
   * For each key k in `this` or `that1` or `that2`, return a resulting SCollection that contains
   * a tuple with the list of values for that key in `this`, `that1` and `that2`.
   * @group cogroup
   */
  def cogroup[W1: Coder, W2: Coder]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    MultiJoin.withName(self.tfName).cogroup(self, that1, that2)

  /**
   * For each key k in `this` or `that1` or `that2` or `that3`, return a resulting SCollection
   * that contains a tuple with the list of values for that key in `this`, `that1`, `that2` and
   * `that3`.
   * @group cogroup
   */
  def cogroup[W1: Coder, W2: Coder, W3: Coder]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    MultiJoin.withName(self.tfName).cogroup(self, that1, that2, that3)

  /**
   * Alias for `cogroup`.
   * @group cogroup
   */
  def groupWith[W: Coder](that: SCollection[(K, W)])
  : SCollection[(K, (Iterable[V], Iterable[W]))] =
    this.cogroup(that)

  /**
   * Alias for `cogroup`.
   * @group cogroup
   */
  def groupWith[W1: Coder, W2: Coder]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    this.cogroup(that1, that2)

  /**
   * Alias for `cogroup`.
   * @group cogroup
   */
  def groupWith[W1: Coder, W2: Coder, W3: Coder]
  (that1: SCollection[(K, W1)], that2: SCollection[(K, W2)], that3: SCollection[(K, W3)])
  : SCollection[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    this.cogroup(that1, that2, that3)

  // =======================================================================
  // Joins
  // =======================================================================

  /**
   * Perform a full outer join of `this` and `that`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), Some(w))) for w in `that`,
   * or the pair (k, (Some(v), None)) if no elements in `that` have key k. Similarly, for each
   * element (k, w) in `that`, the resulting SCollection will either contain all pairs (k,
   * (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements in
   * `this` have key k.
   * @group join
   */
  def fullOuterJoin[W: Coder](that: SCollection[(K, W)])
  : SCollection[(K, (Option[V], Option[W]))] =
    ArtisanJoin.outer(self.tfName, self, that)

  /**
   * Return an SCollection containing all pairs of elements with matching keys in `this` and
   * `that`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in
   * `this` and (k, v2) is in `that`.
   * @group join
   */
  def join[W: Coder](that: SCollection[(K, W)]): SCollection[(K, (V, W))] =
    ArtisanJoin(self.tfName, self, that)

  /**
   * Perform a left outer join of `this` and `that`. For each element (k, v) in `this`, the
   * resulting SCollection will either contain all pairs (k, (v, Some(w))) for w in `that`, or the
   * pair (k, (v, None)) if no elements in `that` have key k.
   * @group join
   */
  def leftOuterJoin[W: Coder](that: SCollection[(K, W)]): SCollection[(K, (V, Option[W]))] =
    ArtisanJoin.left(self.tfName, self, that)

  /**
   * Perform a right outer join of `this` and `that`. For each element (k, w) in `that`, the
   * resulting SCollection will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k.
   * @group join
   */
  def rightOuterJoin[W: Coder](that: SCollection[(K, W)])
  : SCollection[(K, (Option[V], W))] =
    ArtisanJoin.right(self.tfName, self, that)

  /* Hash operations */

  /**
   * Perform an inner join by replicating `that` to all workers. The right side should be tiny and
   * fit in memory.
   *
   * @group join
   */
  def hashJoin[W: Coder](that: SCollection[(K, W)])
  : SCollection[(K, (V, W))] = self.transform { in =>
    implicitly[Coder[K]]
    implicitly[Coder[ArrayBuffer[W]]]
    implicitly[Coder[MMap[K, ArrayBuffer[W]]]]
    val side = that.combine { case (k, v) =>
      MMap(k -> ArrayBuffer(v))
    } { case (combiner, (k, v)) =>
      combiner.getOrElseUpdate(k, ArrayBuffer.empty[W]) += v
      combiner
    } { case (left, right) =>
        right.foreach { case (k, vs) => left.getOrElseUpdate(k, ArrayBuffer.empty[W]) ++= vs }
        left
    }.asSingletonSideInput(MMap.empty[K, ArrayBuffer[W]])

    in.withSideInputs(side).flatMap[(K, (V, W))] { (kv, s) =>
      s(side).getOrElse(kv._1, ArrayBuffer.empty[W]).iterator.map(w => (kv._1, (kv._2, w)))
    }.toSCollection
  }

  /**
   * Perform a left outer join by replicating `that` to all workers. The right side should be tiny
   * and fit in memory.
   *
   * @group join
   */
  def hashLeftJoin[W: Coder](that: SCollection[(K, W)])
  : SCollection[(K, (V, Option[W]))] = self.transform { in =>
    val side = that.combine { case (k, v) =>
      MMap(k -> ArrayBuffer(v))
    } { case (combiner, (k, v)) =>
      combiner.getOrElseUpdate(k, ArrayBuffer.empty[W]) += v
      combiner
    } { case (left, right) =>
      right.foreach { case (k, vs) => left.getOrElseUpdate(k, ArrayBuffer.empty[W]) ++= vs }
      left
    }.asSingletonSideInput(MMap.empty[K, ArrayBuffer[W]])

    in.withSideInputs(side).flatMap[(K, (V, Option[W]))] { (kv, s) =>
      val (k, v) = kv
      val m = s(side)
      if (m.contains(k)) m(k).iterator.map(w => (k, (v, Some(w)))) else Iterator((k, (v, None)))
    }.toSCollection
  }

  // scalastyle:off parameter.number
  /**
   * N to 1 skew-proof flavor of [[join]].
   *
   * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
   * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
   * estimate is within `eps * N` of the true frequency.
   * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
   * the left hand side stream so far.
   *
   * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example {{{
   * // Implicits that enabling CMS-hashing
   * import com.twitter.algebird.CMSHasherImplicits._
   *
   * val p = logs.skewedJoin(logMetadata)
   * }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
   * @group join
   * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
   *                        have inefficient `GroupByKey` implementation for groups with more than
   *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
   *                        10K, keep upper estimation error in mind. If you sample input via
   *                        `sampleFraction` make sure to adjust `hotKeyThreshold` accordingly.
   * @param eps One-sided error bound on the error of each point query, i.e. frequency estimate.
   *            Must lie in `(0, 1)`.
   * @param seed A seed to initialize the random number generator used to create the pairwise
   *             independent hash functions.
   * @param delta A bound on the probability that a query estimate does not lie within some small
   *              interval (an interval that depends on `eps`) around the truth. Must lie in
   *              `(0, 1)`.
   * @param sampleFraction left side sample fraction. Default is `1.0` - no sampling.
   * @param withReplacement whether to use sampling with replacement, see
   *                        [[SCollection.sample(withReplacement:Boolean,fraction:Double)*
   *                        SCollection.sample]].
   */
  def skewedJoin[W: Coder](that: SCollection[(K, W)],
                              hotKeyThreshold: Long = 9000,
                              eps: Double = 0.001,
                              seed: Int = 42,
                              delta: Double = 1E-10,
                              sampleFraction: Double = 1.0,
                              withReplacement: Boolean = true)(implicit hasher: CMSHasher[K])
  : SCollection[(K, (V, W))] = {
    require(sampleFraction <= 1.0 && sampleFraction > 0.0,
      "Sample fraction has to be between (0.0, 1.0] - default is 1.0")

    import com.twitter.algebird._
    // Key aggregator for `k->#values`
    // TODO: might be better to use SparseCMS
    val keyAggregator = CMS.aggregator[K](eps, delta, seed)

    val leftSideKeys = if (sampleFraction < 1.0) {
      self.withName("Sample LHS").sample(withReplacement, sampleFraction).keys
    } else {
      self.keys
    }

    val cms = leftSideKeys.withName("Compute CMS of LHS keys").aggregate(keyAggregator)
    self.skewedJoin(that, hotKeyThreshold, cms)
  }
  // scalastyle:on parameter.number

  /**
   * N to 1 skew-proof flavor of [[join]].
   *
   * Perform a skewed join where some keys on the left hand may be hot, i.e. appear more than
   * `hotKeyThreshold` times. Frequency of a key is estimated with `1 - delta` probability, and the
   * estimate is within `eps * N` of the true frequency.
   * `true frequency <= estimate <= true frequency + eps * N`, where N is the total size of
   * the left hand side stream so far.
   *
   * @note Make sure to `import com.twitter.algebird.CMSHasherImplicits` before using this join.
   * @example {{{
   * // Implicits that enabling CMS-hashing
   * import com.twitter.algebird.CMSHasherImplicits._
   *
   * val keyAggregator = CMS.aggregator[K](eps, delta, seed)
   * val hotKeyCMS = self.keys.aggregate(keyAggregator)
   * val p = logs.skewedJoin(logMetadata, hotKeyThreshold = 8500, cms=hotKeyCMS)
   * }}}
   *
   * Read more about CMS: [[com.twitter.algebird.CMSMonoid]].
   * @group join
   * @param hotKeyThreshold key with `hotKeyThreshold` values will be considered hot. Some runners
   *                        have inefficient `GroupByKey` implementation for groups with more than
   *                        10K values. Thus it is recommended to set `hotKeyThreshold` to below
   *                        10K, keep upper estimation error in mind.
   * @param cms left hand side key [[com.twitter.algebird.CMSMonoid]]
   */
  def skewedJoin[W: Coder](that: SCollection[(K, W)],
                              hotKeyThreshold: Long,
                              cms: SCollection[CMS[K]])
  : SCollection[(K, (V, W))] = {
    val (hotSelf, chillSelf) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
    // scalastyle:off line.size.limit
    // Use asIterableSideInput as workaround for:
    // http://stackoverflow.com/questions/37126729/ismsinkwriter-expects-keys-to-be-written-in-strictly-increasing-order
    // scalastyle:on line.size.limit
    val keyCMS = cms.asIterableSideInput
    val error = cms
      .withName("Compute CMS error bound")
      .map(c => c.totalCount * c.eps).asSingletonSideInput

    val partitionedSelf = self
      .withSideInputs(keyCMS, error)
      .transformWithSideOutputs(Seq(hotSelf, chillSelf), "Partition LHS") { (e, c) =>
        if (c(keyCMS).nonEmpty &&
            c(keyCMS).head.frequency(e._1).estimate >= c(error) + hotKeyThreshold) {
          hotSelf
        } else {
          chillSelf
        }
      }

    val (hotThat, chillThat) = (SideOutput[(K, W)](), SideOutput[(K, W)]())
    val partitionedThat = that
      .withSideInputs(keyCMS, error)
      .transformWithSideOutputs(Seq(hotThat, chillThat), "Partition RHS") { (e, c) =>
        if (c(keyCMS).nonEmpty &&
            c(keyCMS).head.frequency(e._1).estimate >= c(error) + hotKeyThreshold) {
          hotThat
        } else {
          chillThat
        }
      }

    // Use hash join for hot keys
    val hotJoined = partitionedSelf(hotSelf)
      .withName("Hash join hot partitions")
      .hashJoin(partitionedThat(hotThat))

    // Use regular join for the rest of the keys
    val chillJoined = partitionedSelf(chillSelf)
      .withName("Join chill partitions")
      .join(partitionedThat(chillThat))

    hotJoined.withName("Union hot and chill join results") ++ chillJoined
  }

  /**
   * Full outer join for cases when `this` is much larger than `that` which cannot fit in memory,
   * but contains a mostly overlapping set of keys as `this`, i.e. when the intersection of keys
   * is sparse in `this`. A Bloom Filter of keys in `that` is used to split `this` into 2
   * partitions. Only those with keys in the filter go through the join and the rest are
   * concatenated. This is useful for joining historical aggregates with incremental updates.
   * Read more about Bloom Filter: [[com.twitter.algebird.BloomFilter]].
   * @group join
   * @param thatNumKeys estimated number of keys in `that`
   * @param fpProb false positive probability when computing the overlap
   */
  def sparseOuterJoin[W: Coder](that: SCollection[(K, W)],
                                   thatNumKeys: Long,
                                   fpProb: Double = 0.01)
                                  (implicit hash: Hash128[K])
  : SCollection[(K, (Option[V], Option[W]))] = {
    val bfSettings = PairSCollectionFunctions.optimalBFSettings(thatNumKeys, fpProb)
    if (bfSettings.numBFs == 1) {
      sparseOuterJoinImpl(that, thatNumKeys.toInt, fpProb)
    } else {
      val n = bfSettings.numBFs
      val thisParts = self.partition(n, _._1.hashCode() % n)
      val thatParts = that.partition(n, _._1.hashCode() % n)
      val joined = (thisParts zip thatParts).map { case (lhs, rhs) =>
        lhs.sparseOuterJoinImpl(rhs, bfSettings.capacity, fpProb)
      }
      SCollection.unionAll(joined)
    }
  }

  protected def sparseOuterJoinImpl[W: Coder](that: SCollection[(K, W)],
                                                 thatNumKeys: Int,
                                                 fpProb: Double)
                                                (implicit hash: Hash128[K])
  : SCollection[(K, (Option[V], Option[W]))] = {
    val width = BloomFilter.optimalWidth(thatNumKeys, fpProb).get
    val numHashes = BloomFilter.optimalNumHashes(thatNumKeys, width)
    val rhsBf = that.keys.aggregate(BloomFilterAggregator[K](numHashes, width)).asIterableSideInput
    val (lhsUnique, lhsOverlap) = (SideOutput[(K, V)](), SideOutput[(K, V)]())
    val partitionedSelf = self
      .withSideInputs(rhsBf)
      .transformWithSideOutputs(Seq(lhsUnique, lhsOverlap)) { (e, c) =>
        if (c(rhsBf).nonEmpty && c(rhsBf).head.maybeContains(e._1)) {
          lhsOverlap
        } else {
          lhsUnique
        }
      }
    val unique = partitionedSelf(lhsUnique).map(kv => (kv._1, (Option(kv._2), Option.empty[W])))
    val overlap = partitionedSelf(lhsOverlap).fullOuterJoin(that)
    unique ++ overlap
  }

  // =======================================================================
  // Transformations
  // =======================================================================

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, `U`, than the type of the values in this
   * SCollection, `V`. Thus, we need one operation for merging a `V` into a `U` and one operation
   * for merging two `U``'s. To avoid memory allocation, both of these functions are allowed to
   * modify and return their first argument instead of creating a new `U`.
   * @group per_key
   */
  def aggregateByKey[U: Coder](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U): SCollection[(K, U)] =
    this.applyPerKey(
      Combine.perKey(Functions.aggregateFn(zeroValue)(seqOp, combOp)),
      kvToTuple[K, U])

  /**
   * Aggregate the values of each key with [[com.twitter.algebird.Aggregator Aggregator]]. First
   * each value `V` is mapped to `A`, then we reduce with a
   * [[com.twitter.algebird.Semigroup Semigroup]] of `A`, then finally we present the results as
   * `U`. This could be more powerful and better optimized in some cases.
   * @group per_key
   */
  def aggregateByKey[A: Coder, U: Coder](aggregator: Aggregator[V, A, U])
  : SCollection[(K, U)] = self.transform { in =>
    val a = aggregator  // defeat closure
    in.mapValues(a.prepare).sumByKey(a.semigroup).mapValues(a.present)
  }

  /**
   * For each key, compute the values' data distribution using approximate `N`-tiles.
   * @return a new SCollection whose values are `Iterable`s of the approximate `N`-tiles of
   * the elements.
   * @group per_key
   */
  def approxQuantilesByKey(numQuantiles: Int)(implicit ord: Ordering[V])
  : SCollection[(K, Iterable[V])] =
    this.applyPerKey(
      ApproximateQuantiles.perKey(numQuantiles, ord),
      kvListToTuple[K, V])

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an `SCollection[(K, V)]` into a result of type `SCollection[(K, C)]`, for a
   * "combined type" `C` Note that `V` and `C` can be different -- for example, one might group an
   * SCollection of type `(Int, Int)` into an SCollection of type `(Int, Seq[Int])`. Users provide
   * three functions:
   *
   * - `createCombiner`, which turns a `V` into a `C` (e.g., creates a one-element list)
   *
   * - `mergeValue`, to merge a `V` into a `C` (e.g., adds it to the end of a list)
   *
   * - `mergeCombiners`, to combine two `C`'s into a single one.
   * @group per_key
   */
  def combineByKey[C: Coder](createCombiner: V => C)
                               (mergeValue: (C, V) => C)
                               (mergeCombiners: (C, C) => C): SCollection[(K, C)] =
    this.applyPerKey(
      Combine.perKey(Functions.combineFn(createCombiner, mergeValue, mergeCombiners)),
      kvToTuple[K, C])

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param sampleSize the number of entries in the statistical sample; the higher this number, the
   * more accurate the estimate will be; should be `>= 16`.
   * @group per_key
   */
  def countApproxDistinctByKey(sampleSize: Int): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](sampleSize), klToTuple[K])

  /**
   * Count approximate number of distinct values for each key in the SCollection.
   * @param maximumEstimationError the maximum estimation error, which should be in the range
   * `[0.01, 0.5]`.
   * @group per_key
   */
  def countApproxDistinctByKey(maximumEstimationError: Double = 0.02): SCollection[(K, Long)] =
    this.applyPerKey(ApproximateUnique.perKey[K, V](maximumEstimationError), klToTuple[K])

  /**
   * Count the number of elements for each key.
   * @return a new SCollection of (key, count) pairs
   * @group per_key
   */
  def countByKey: SCollection[(K, Long)] = self.transform(_.keys.countByValue)

  /**
   * Pass each value in the key-value pair SCollection through a `flatMap` function without
   * changing the keys.
   * @group transform
   */
  def flatMapValues[U: Coder](f: V => TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMap(kv => f(kv._2).map(v => (kv._1, v)))

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   * @group per_key
   */
  def foldByKey(zeroValue: V)(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.aggregateFn(zeroValue)(op, op)), kvToTuple[K, V])

  /**
   * Fold by key with [[com.twitter.algebird.Monoid Monoid]], which defines the associative
   * function and "zero value" for `V`. This could be more powerful and better optimized in some
   * cases.
   * @group per_key
   */
  def foldByKey(implicit mon: Monoid[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(mon)), kvToTuple[K, V])

  /**
   * Group the values for each key in the SCollection into a single sequence. The ordering of
   * elements within each group is not guaranteed, and may even differ each time the resulting
   * SCollection is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using
   * [[PairSCollectionFunctions.aggregateByKey[U]* PairSCollectionFunctions.aggregateByKey]] or
   * [[PairSCollectionFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, `groupByKey` must be able to hold all the key-value pairs for
   * any key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
   * @group per_key
   */
  def groupByKey: SCollection[(K, Iterable[V])] =
    this.applyPerKey(GroupByKey.create[K, V](), kvIterableToTuple[K, V])

  /**
   * Return an SCollection with the pairs from `this` whose keys are in `that`.
   * @group per_key
   */
  def intersectByKey(that: SCollection[K]): SCollection[(K, V)] = self.transform {
    _.cogroup(that.map((_, ()))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.nonEmpty) t._2._1.map((t._1, _)) else Seq.empty
    }
  }

  /**
   * Return an SCollection with the keys of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Keys
  def keys: SCollection[K] = self.map(_._1)

  /**
   * Pass each value in the key-value pair SCollection through a `map` function without changing
   * the keys.
   * @group transform
   */
  def mapValues[U: Coder](f: V => U): SCollection[(K, U)] = self.map(kv => (kv._1, f(kv._2)))

  /**
   * Return the max of values for each key as defined by the implicit `Ordering[T]`.
   * @return a new SCollection of (key, maximum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Max
  def maxByKey(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.max)

  /**
   * Return the min of values for each key as defined by the implicit `Ordering[T]`.
   * @return a new SCollection of (key, minimum value) pairs
   * @group per_key
   */
  // Scala lambda is simpler and more powerful than transforms.Min
  def minByKey(implicit ord: Ordering[V]): SCollection[(K, V)] = this.reduceByKey(ord.min)

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   * @group per_key
   */
  def reduceByKey(op: (V, V) => V): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(op)), kvToTuple[K, V])

  /**
   * Return a sampled subset of values for each key of this SCollection.
   * @return a new SCollection of (key, sampled values) pairs
   * @group per_key
   */
  def sampleByKey(sampleSize: Int): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Sample.fixedSizePerKey[K, V](sampleSize), kvIterableToTuple[K, V])

  /**
   * Return a subset of this SCollection sampled by key (via stratified sampling).
   *
   * Create a sample of this SCollection using variable sampling rates for different keys as
   * specified by `fractions`, a key to sampling rate map, via simple random sampling with one
   * pass over the SCollection, to produce a sample of size that's approximately equal to the sum
   * of `math.ceil(numItems * samplingRate)` over all key values.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @return SCollection containing the sampled subset
   * @group per_key
   */
  def sampleByKey(withReplacement: Boolean, fractions: Map[K, Double]): SCollection[(K, V)] = {
    if (withReplacement) {
      self.parDo(new PoissonValueSampler[K, V](fractions))
    } else {
      self.parDo(new BernoulliValueSampler[K, V](fractions))
    }
  }

  /**
   * Return an SCollection with the pairs from `this` whose keys are not in `that`.
   * @group per_key
   */
  def subtractByKey(that: SCollection[K]): SCollection[(K, V)] = self.transform {
    _.cogroup(that.map((_, ()))).flatMap { t =>
      if (t._2._1.nonEmpty && t._2._2.isEmpty) t._2._1.map((t._1, _)) else Seq.empty
    }
  }

  /**
   * Reduce by key with [[com.twitter.algebird.Semigroup Semigroup]]. This could be more powerful
   * and better optimized than [[reduceByKey]] in some cases.
   * @group per_key
   */
  def sumByKey(implicit sg: Semigroup[V]): SCollection[(K, V)] =
    this.applyPerKey(Combine.perKey(Functions.reduceFn(sg)), kvToTuple[K, V])

  /**
   * Swap the keys with the values.
   * @group transform
   */
  // Scala lambda is simpler than transforms.KvSwap
  def swap: SCollection[(V, K)] = self.map(kv => (kv._2, kv._1))

  /**
   * Return the top k (largest) values for each key from this SCollection as defined by the
   * specified implicit `Ordering[T]`.
   * @return a new SCollection of (key, top k) pairs
   * @group per_key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): SCollection[(K, Iterable[V])] =
    this.applyPerKey(Top.perKey[K, V, Ordering[V]](num, ord), kvListToTuple[K, V])

  /**
   * Return an SCollection with the values of each tuple.
   * @group transform
   */
  // Scala lambda is simpler and more powerful than transforms.Values
  def values: SCollection[V] = self.map(_._2)

  /**
   * Return an SCollection having its values flattened.
   * @group transform
   */
  def flattenValues[U: Coder](implicit ev: V <:< TraversableOnce[U]): SCollection[(K, U)] =
    self.flatMapValues(_.asInstanceOf[TraversableOnce[U]])

  // =======================================================================
  // Side input operations
  // =======================================================================

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
   * `Map[key, value]`, to be used with [[SCollection.withSideInputs]]. It is required that each
   * key of the input be associated with a single value.
   *
   * Currently, the resulting map is required to fit into memory.
   */
  def asMapSideInput: SideInput[Map[K, V]] = {
    val o = self.applyInternal(
      new PTransform[PCollection[(K, V)], PCollectionView[JMap[K, V]]]() {
        override def expand(input: PCollection[(K, V)]): PCollectionView[JMap[K, V]] = {
          input.apply(toKvTransform).setCoder(kvCoder[K, V]).apply(View.asMap())
        }
      })
    new MapSideInput[K, V](o)
  }

  /**
   * Convert this SCollection to a SideInput, mapping key-value pairs of each window to a
   * `Map[key, Iterable[value]]`, to be used with [[SCollection.withSideInputs]]. In contrast to
   * [[asMapSideInput]], it is not required that the keys in the input collection be unique.
   *
   * Currently, the resulting map is required to fit into memory.
   */
  def asMultiMapSideInput: SideInput[Map[K, Iterable[V]]] = {
    val o = self.applyInternal(
      new PTransform[PCollection[(K, V)], PCollectionView[JMap[K, JIterable[V]]]]() {
        override def expand(input: PCollection[(K, V)]): PCollectionView[JMap[K, JIterable[V]]] = {
          input.apply(toKvTransform).setCoder(kvCoder[K, V]).apply(View.asMultimap())
        }
      })
    new MultiMapSideInput[K, V](o)
  }

}
// scalastyle:on number.of.methods
