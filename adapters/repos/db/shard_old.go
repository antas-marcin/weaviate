//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/docid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/noop"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
	"golang.org/x/sync/errgroup"
)

func NewShard_old(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
) (*Shard, error) {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	before := time.Now()

	s := &Shard{
		index:       index,
		name:        shardName,
		promMetrics: promMetrics,
		metrics: NewMetrics(index.logger, promMetrics,
			string(index.Config.ClassName), shardName),
		stopMetrics:     make(chan struct{}),
		replicationMap:  pendingReplicaTasks{Tasks: make(map[string]replicaTask, 32)},
		centralJobQueue: jobQueueCh,
	}
	s.initCycleCallbacks()

	s.docIdLock = make([]sync.Mutex, IdLockPoolSize)

	defer s.metrics.ShardStartup(before)

	hnswUserConfig, ok := index.vectorIndexUserConfig.(hnswent.UserConfig)
	if !ok {
		return nil, errors.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
			index.vectorIndexUserConfig)
	}

	if hnswUserConfig.Skip {
		s.vectorIndex = noop.NewIndex()
	} else {
		if err := s.initVectorIndex_old(ctx, hnswUserConfig); err != nil {
			return nil, fmt.Errorf("init vector index: %w", err)
		}

		defer s.vectorIndex.PostStartup()
	}

	if err := s.initNonVector_old(ctx, class); err != nil {
		return nil, errors.Wrapf(err, "init shard %q", s.ID())
	}

	return s, nil
}

func (s *Shard) initVectorIndex_old(
	ctx context.Context, hnswUserConfig hnswent.UserConfig,
) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	var distProv distancer.Provider

	switch hnswUserConfig.Distance {
	case "", hnswent.DistanceCosine:
		distProv = distancer.NewCosineDistanceProvider()
	case hnswent.DistanceDot:
		distProv = distancer.NewDotProductProvider()
	case hnswent.DistanceL2Squared:
		distProv = distancer.NewL2SquaredProvider()
	case hnswent.DistanceManhattan:
		distProv = distancer.NewManhattanProvider()
	case hnswent.DistanceHamming:
		distProv = distancer.NewHammingProvider()
	default:
		return errors.Errorf("unrecognized distance metric %q,"+
			"choose one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]", hnswUserConfig.Distance)
	}

	// starts vector cycles if vector is configured
	s.index.cycleCallbacks.vectorCommitLoggerCycle.Start()
	s.index.cycleCallbacks.vectorTombstoneCleanupCycle.Start()

	vi, err := hnsw.New(hnsw.Config{
		Logger:               s.index.logger,
		RootPath:             s.index.Config.RootPath,
		ID:                   s.ID(),
		ShardName:            s.name,
		ClassName:            s.index.Config.ClassName.String(),
		PrometheusMetrics:    s.promMetrics,
		VectorForIDThunk:     s.vectorByIndexID,
		TempVectorForIDThunk: s.readVectorByIndexIDIntoSlice,
		DistanceProvider:     distProv,
		MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(s.index.Config.RootPath, s.ID(),
				s.index.logger, s.cycleCallbacks.vectorCommitLoggerCallbacks)
		},
	}, hnswUserConfig,
		s.cycleCallbacks.vectorTombstoneCleanupCallbacks, s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: hnsw index", s.ID())
	}
	s.vectorIndex = vi

	return nil
}

func (s *Shard) initNonVector_old(ctx context.Context, class *models.Class) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	err := s.initLSMStore_old(ctx)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: shard db", s.ID())
	}

	counter, err := indexcounter.New(s.ID(), s.index.Config.RootPath)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: index counter", s.ID())
	}
	s.counter = counter

	dataPresent := s.counter.PreviewNext() != 0
	versionPath := path.Join(s.index.Config.RootPath, s.ID()+".version")
	versioner, err := newShardVersioner(versionPath, dataPresent)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: check versions", s.ID())
	}
	s.versioner = versioner

	plPath := path.Join(s.index.Config.RootPath, s.ID()+".proplengths")
	propLengths, err := inverted.NewJsonPropertyLengthTracker(plPath, s.index.logger)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: prop length tracker", s.ID())
	}
	s.propLengths = propLengths

	piPath := path.Join(s.index.Config.RootPath, s.ID()+".propids")
	propIds, err := tracker.NewJsonPropertyIdTracker(piPath)
	if err != nil {
		return errors.Wrapf(err, "init shard %q: prop id tracker", s.ID())
	}
	s.propIds = propIds

	if err := s.initProperties_old(class); err != nil {
		return errors.Wrapf(err, "init shard %q: init per property indices", s.ID())
	}

	s.initDimensionTracking()

	return nil
}

func (s *Shard) ID_old() string {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return fmt.Sprintf("%s_%s", s.index.ID(), s.name)
}

func (s *Shard) DBPathLSM_old() string {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return fmt.Sprintf("%s/%s_lsm", s.index.Config.RootPath, s.ID())
}

func (s *Shard) uuidToIdLockPoolId_old(idBytes []byte) uint8 {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	// use the last byte of the uuid to determine which locking-pool a given object should use. The last byte is used
	// as uuids probably often have some kind of order and the last byte will in general be the one that changes the most
	return idBytes[15] % IdLockPoolSize
}

func (s *Shard) initLSMStore_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	annotatedLogger := s.index.logger.WithFields(logrus.Fields{
		"shard": s.name,
		"index": s.index.ID(),
		"class": s.index.Config.ClassName,
	})
	var metrics *lsmkv.Metrics
	if s.promMetrics != nil {
		metrics = lsmkv.NewMetrics(s.promMetrics, string(s.index.Config.ClassName), s.name)
	}

	store, err := lsmkv.New(s.DBPathLSM(), s.index.Config.RootPath, annotatedLogger, metrics,
		s.cycleCallbacks.compactionCallbacks, s.cycleCallbacks.flushCallbacks)
	if err != nil {
		return errors.Wrapf(err, "init lsmkv store at %s", s.DBPathLSM())
	}

	err = store.CreateOrLoadBucket_old(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithMonitorCount(),
		s.dynamicMemtableSizing_old(),
		s.memtableIdleConfig_old(),
	)
	if err != nil {
		return errors.Wrap(err, "create objects bucket")
	}

	s.store = store

	return nil
}

func (s *Shard) drop_old() error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	s.replicationMap.clear()

	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
		if s.promMetrics != nil {
			// send 0 in when index gets dropped
			s.sendVectorDimensionsMetric(0)
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	if err := s.cycleCallbacks.vectorCombinedCallbacksCtrl.Unregister(ctx); err != nil {
		return fmt.Errorf("drop shard '%s': %w", s.name, err)
	}
	if err := s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Unregister(ctx); err != nil {
		return fmt.Errorf("drop shard '%s': %w", s.name, err)
	}

	if err := s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	if _, err := os.Stat(s.DBPathLSM()); err == nil {
		err := os.RemoveAll(s.DBPathLSM())
		if err != nil {
			return errors.Wrapf(err, "remove lsm store at %s", s.DBPathLSM())
		}
	}
	// delete indexcount
	err := s.counter.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.DBPathLSM())
	}

	// delete indexcount
	err = s.versioner.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove indexcount at %s", s.DBPathLSM())
	}
	// remove vector index
	err = s.vectorIndex.Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "remove vector index at %s", s.DBPathLSM())
	}

	// delete indexcount
	err = s.propLengths.Drop()
	if err != nil {
		return errors.Wrapf(err, "remove prop length tracker at %s", s.DBPathLSM())
	}

	s.propertyIndicesLock.Lock()
	err = s.propertyIndices.DropAll(ctx)
	s.propertyIndicesLock.Unlock()
	if err != nil {
		return errors.Wrapf(err, "remove property specific indices at %s", s.DBPathLSM())
	}

	return nil
}

func (s *Shard) addIDProperty_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket_old(ctx,
		helpers.BucketFromPropertyNameLSM(filters.InternalPropID),
		lsmkv.WithIdleThreshold(time.Duration(s.index.Config.MemtablesFlushIdleAfter)*time.Second),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection))
}

func (s *Shard) addDimensionsProperty_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	// Note: this data would fit the "Set" type better, but since the "Map" type
	// is currently optimized better, it is more efficient to use a Map here.
	err := s.store.CreateOrLoadBucket_old(ctx,
		helpers.DimensionsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection))
	if err != nil {
		return err
	}

	return nil
}

func (s *Shard) addTimestampProperties_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if err := s.addCreationTimeUnixProperty_old(ctx); err != nil {
		return err
	}
	if err := s.addLastUpdateTimeUnixProperty_old(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addCreationTimeUnixProperty_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return s.store.CreateOrLoadBucket_old(ctx,
		helpers.BucketFromPropertyNameLSM(filters.InternalPropCreationTimeUnix),
		lsmkv.WithIdleThreshold(time.Duration(s.index.Config.MemtablesFlushIdleAfter)*time.Second),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
}

func (s *Shard) addLastUpdateTimeUnixProperty_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return s.store.CreateOrLoadBucket_old(ctx,
		helpers.BucketFromPropertyNameLSM(filters.InternalPropLastUpdateTimeUnix),
		lsmkv.WithIdleThreshold(time.Duration(s.index.Config.MemtablesFlushIdleAfter)*time.Second),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
}

func (s *Shard) memtableIdleConfig_old() lsmkv.BucketOption {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return lsmkv.WithIdleThreshold(
		time.Duration(s.index.Config.MemtablesFlushIdleAfter) * time.Second)
}

func (s *Shard) dynamicMemtableSizing_old() lsmkv.BucketOption {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return lsmkv.WithDynamicMemtableSizing(
		s.index.Config.MemtablesInitialSizeMB,
		s.index.Config.MemtablesMaxSizeMB,
		s.index.Config.MemtablesMinActiveSeconds,
		s.index.Config.MemtablesMaxActiveSeconds,
	)
}

func (s *Shard) createPropertyIndex_old(ctx context.Context, prop *models.Property, eg *errgroup.Group) {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if !inverted.HasInvertedIndex(prop) {
		return
	}

	eg.Go(func() error {
		if err := s.createPropertyValueIndex_old(ctx, prop); err != nil {
			return errors.Wrapf(err, "create property '%s' value index on shard '%s'", prop.Name, s.ID())
		}

		if s.index.invertedIndexConfig.IndexNullState {
			eg.Go(func() error {
				if err := s.createPropertyNullIndex_old(ctx, prop); err != nil {
					return errors.Wrapf(err, "create property '%s' null index on shard '%s'", prop.Name, s.ID())
				}
				return nil
			})
		}

		if s.index.invertedIndexConfig.IndexPropertyLength {
			eg.Go(func() error {
				if err := s.createPropertyLengthIndex_old(ctx, prop); err != nil {
					return errors.Wrapf(err, "create property '%s' length index on shard '%s'", prop.Name, s.ID())
				}
				return nil
			})
		}

		return nil
	})
}

func (s *Shard) createPropertyValueIndex_old(ctx context.Context, prop *models.Property) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	bucketOpts := []lsmkv.BucketOption{
		s.memtableIdleConfig_old(),
		s.dynamicMemtableSizing_old(),
	}

	if inverted.HasFilterableIndex(prop) {
		if dt, _ := schema.AsPrimitive(prop.DataType); dt == schema.DataTypeGeoCoordinates {
			return s.initGeoProp(prop)
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := s.store.CreateOrLoadBucket_old(ctx,
				helpers.BucketFromPropertyNameMetaCountLSM(prop.Name),
				append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
			); err != nil {
				return err
			}
		}

		if err := s.store.CreateOrLoadBucket_old(ctx,
			helpers.BucketFromPropertyNameLSM(prop.Name),
			append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
		); err != nil {
			return err
		}
	}

	if inverted.HasSearchableIndex(prop) {
		searchableBucketOpts := append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyMapCollection))
		if s.versioner.Version() < 2 {
			searchableBucketOpts = append(searchableBucketOpts, lsmkv.WithLegacyMapSorting())
		}

		if err := s.store.CreateOrLoadBucket_old(ctx,
			helpers.BucketSearchableFromPropertyNameLSM(prop.Name),
			searchableBucketOpts...,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) createPropertyLengthIndex_old(ctx context.Context, prop *models.Property) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	// some datatypes are not added to the inverted index, so we can skip them here
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeInt,
		schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return nil
	default:
	}

	return s.store.CreateOrLoadBucket_old(ctx,
		helpers.BucketFromPropertyNameLengthLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
}

func (s *Shard) createPropertyNullIndex_old(ctx context.Context, prop *models.Property) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket_old(ctx,
		helpers.BucketFromPropertyNameNullLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
}

func (s *Shard) updateVectorIndexConfig_old(ctx context.Context,
	updated schema.VectorIndexConfig,
) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	err := s.updateStatus(storagestate.StatusReadOnly.String())
	if err != nil {
		return fmt.Errorf("attempt to mark read-only: %w", err)
	}
	return s.vectorIndex.UpdateUserConfig(updated, func() {
		s.updateStatus(storagestate.StatusReady.String())
	})
}

func (s *Shard) shutdown_old(ctx context.Context) error {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	if s.index.Config.TrackVectorDimensions {
		// tracking vector dimensions goroutine only works when tracking is enabled
		// that's why we are trying to stop it only in this case
		s.stopMetrics <- struct{}{}
	}

	if err := s.propLengths.Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker")
	}

	// to ensure that all commitlog entries are written to disk.
	// otherwise in some cases the tombstone cleanup process'
	// 'RemoveTombstone' entry is not picked up on restarts
	// resulting in perpetually attempting to remove a tombstone
	// which doesn't actually exist anymore
	if err := s.vectorIndex.Flush(); err != nil {
		return errors.Wrap(err, "flush vector index commitlog")
	}

	if err := s.vectorIndex.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "shut down vector index")
	}

	if err := s.cycleCallbacks.compactionCallbacksCtrl.Unregister(ctx); err != nil {
		return err
	}
	if err := s.cycleCallbacks.flushCallbacksCtrl.Unregister(ctx); err != nil {
		return err
	}
	if err := s.cycleCallbacks.vectorCombinedCallbacksCtrl.Unregister(ctx); err != nil {
		return err
	}
	if err := s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Unregister(ctx); err != nil {
		return err
	}

	if err := s.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "stop lsmkv store")
	}

	return nil
}

func (s *Shard) notifyReady_old() {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	s.initStatus()
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}

func (s *Shard) objectCount_old() int {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	b := s.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return 0
	}

	return b.Count()
}

func (s *Shard) isFallbackToSearchable_old() bool {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	return s.fallbackToSearchable
}

func (s *Shard) tenant_old() string {
	if lsmkv.FeatureUseMergedBuckets {
		panic("merged buckets are not supported in this configuration")
	}
	// TODO provide better impl
	if s.index.partitioningEnabled {
		return s.name
	}
	return ""
}