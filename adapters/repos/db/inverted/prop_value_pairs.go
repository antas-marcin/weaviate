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

package inverted

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"golang.org/x/sync/errgroup"
)

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange *filters.GeoRange
	hasFrequency  bool
	docIDs        docBitmap
	children      []*propValuePair
}

func newPropValuePair() propValuePair {
	return propValuePair{docIDs: newDocBitmap()}
}

func (pv *propValuePair) fetchDocIDs(s *Searcher, limit int, skipCache bool) error {
	if pv.operator.OnValue() {
		id := helpers.BucketFromPropNameLSM(pv.prop)
		if pv.prop == filters.InternalPropBackwardsCompatID {
			// the user-specified ID is considered legacy. we
			// support backwards compatibility with this prop
			id = helpers.BucketFromPropNameLSM(filters.InternalPropID)
			pv.prop = filters.InternalPropID
			pv.hasFrequency = false
		}

		if pv.operator == filters.OperatorIsNull {
			id += filters.InternalNullIndex
		}

		// format of id for property with lengths is "property_len(*PROPNAME*)
		propName, isPropLengthFilter := schema.IsPropertyLength(id, 9)
		if isPropLengthFilter {
			id = helpers.BucketFromPropNameLSM(propName + filters.InternalPropertyLength)
			pv.prop = propName + filters.InternalPropertyLength
		}

		b := s.store.Bucket(id)

		if b == nil && isPropLengthFilter {
			return errors.Errorf("Property length must be indexed to be filterable! " +
				"add `IndexPropertyLength: true` to the invertedIndexConfig." +
				"Geo-coordinates, phone numbers and data blobs are not supported by property length.")
		}

		if b == nil && pv.operator == filters.OperatorIsNull {
			return errors.Errorf("Nullstate must be indexed to be filterable! " +
				"add `indexNullState: true` to the invertedIndexConfig")
		}

		if b == nil && (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) {
			return errors.Errorf("timestamps must be indexed to be filterable! " +
				"add `indexTimestamps: true` to the invertedIndexConfig")
		}

		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			// a nil bucket is ok for a WithinGeoRange filter, as this query is not
			// served by the inverted index, but propagated to a secondary index in
			// .docPointers()
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		ctx := context.TODO() // TODO: pass through instead of spawning new
		dbm, err := s.docBitmap(ctx, b, limit, pv, skipCache)
		if err != nil {
			return err
		}
		pv.docIDs = dbm
	} else {
		eg := errgroup.Group{}
		for i, child := range pv.children {
			i, child := i, child
			eg.Go(func() error {
				// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
				// otherwise we run into situations where each subfilter on their own
				// runs into the limit, possibly yielding in "less than limit" results
				// after merging.
				err := child.fetchDocIDs(s, 0, skipCache)
				if err != nil {
					return errors.Wrapf(err, "nested child %d", i)
				}

				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("nested query: %w", err)
		}
	}

	return nil
}

func (pv *propValuePair) mergeDocIDs(logger logrus.FieldLogger) (*docBitmap, error) {
	if pv.operator.OnValue() {
		return &pv.docIDs, nil
	}

	if pv.operator != filters.OperatorAnd && pv.operator != filters.OperatorOr {
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
	if len(pv.children) == 0 {
		return nil, fmt.Errorf("no children for operator: %s", pv.operator.Name())
	}

	// we have children

	dbms := make([]*docBitmap, len(pv.children))
	for i, child := range pv.children {
		dbm, err := child.mergeDocIDs(logger)
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc bitmap of child %d", i)
		}
		dbms[i] = dbm
	}

	if pv.cacheable() && checksumsIdenticalBM(dbms) {
		// all children are identical, no need to merge, simply return the first
		return dbms[0], nil
	}

	mergeRes := dbms[0].docIDs.Clone()
	mergeFn := mergeRes.And
	if pv.operator == filters.OperatorOr {
		mergeFn = mergeRes.Or
	}

	checksums := make([][]byte, len(pv.children))
	checksums[0] = dbms[0].checksum

	truncate := 20

	logID := rand.Int()

	l := logger.WithFields(logrus.Fields{
		"debug_filter":   true,
		"operator":       pv.operator.Name(),
		"children_count": len(dbms),
		"log_id":         logID,
	})
	for i, bm := range dbms {
		l = l.WithField(fmt.Sprintf("child_%d_docid_count", i), bm.docIDs.GetCardinality())
		l = l.WithField(fmt.Sprintf("child_%d_docid_max", i), bm.docIDs.Maximum())
		l = l.WithField(fmt.Sprintf("child_%d_docid_min", i), bm.docIDs.Minimum())
		if i == truncate {
			break
		}
	}
	l.Infof("before merge - detailed results truncated to %d elements (log_id=%d)", truncate, logID)

	l = logger.WithFields(logrus.Fields{
		"debug_filter":   true,
		"operator":       pv.operator.Name(),
		"children_count": len(dbms),
		"log_id":         logID,
	})
	for i := 1; i < len(dbms); i++ {
		mergeFn(dbms[i].docIDs)

		if i < truncate {
			l = l.WithFields(logrus.Fields{
				fmt.Sprintf("round_%d_merge_status_docid_count", i): mergeRes.GetCardinality(),
				fmt.Sprintf("round_%d_merge_status_docid_max", i):   mergeRes.Maximum(),
				fmt.Sprintf("round_%d_merge_status_docid_min", i):   mergeRes.Minimum(),
			})
		}

		checksums[i] = dbms[i].checksum
	}
	l = l.WithFields(logrus.Fields{
		"final_merge_status_docid_count": mergeRes.GetCardinality(),
		"final_merge_status_docid_max":   mergeRes.Maximum(),
		"final_merge_status_docid_min":   mergeRes.Minimum(),
	})
	l.Infof("merging (intermdiary results truncated to %d) + final result (log_id=%d)", truncate, logID)

	return &docBitmap{
		docIDs:   roaringset.Condense(mergeRes),
		checksum: combineChecksums(checksums, pv.operator),
	}, nil
}

// // if duplicates are acceptable, simpler (and faster) algorithms can be used
// // for merging
// func (pv *propValuePair) mergeDocIDs(acceptDuplicates bool) (*docPointers, error) {
// 	if pv.operator.OnValue() {
// 		return &pv.docIDs, nil
// 	}

// 	switch pv.operator {
// 	case filters.OperatorAnd:
// 		return mergeAndOptimized(pv.children, acceptDuplicates)
// 	case filters.OperatorOr:
// 		return mergeOr(pv.children, acceptDuplicates)
// 	default:
// 		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
// 	}
// }

// // TODO: Delete?
// // This is only left so we can use it as a control or baselines in tests and
// // benchmkarks against the newer optimized version.
// func mergeAnd(children []*propValuePair, acceptDuplicates bool) (*docPointers, error) {
// 	sets := make([]*docPointers, len(children))

// 	// retrieve child IDs
// 	for i, child := range children {
// 		docIDs, err := child.mergeDocIDs(acceptDuplicates)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "retrieve doc ids of child %d", i)
// 		}

// 		sets[i] = docIDs
// 	}

// 	if checksumsIdentical(sets) {
// 		// all children are identical, no need to merge, simply return the first
// 		// set
// 		return sets[0], nil
// 	}

// 	// merge AND
// 	found := map[uint64]uint64{} // map[id]count
// 	for _, set := range sets {
// 		for _, pointer := range set.docIDs {
// 			count := found[pointer]
// 			count++
// 			found[pointer] = count
// 		}
// 	}

// 	var out docPointers
// 	var idsForChecksum []uint64
// 	for id, count := range found {
// 		if count != uint64(len(sets)) {
// 			continue
// 		}

// 		// TODO: optimize to use fixed length slice and cut off (should be
// 		// considerably cheaper on very long lists, such as we encounter during
// 		// large classification cases
// 		out.docIDs = append(out.docIDs, id)
// 		idsForChecksum = append(idsForChecksum, id)
// 	}

// 	checksum, err := docPointerChecksum(idsForChecksum)
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "calculate checksum")
// 	}

// 	out.checksum = checksum
// 	return &out, nil
// }

// func mergeOr(children []*propValuePair, acceptDuplicates bool) (*docPointers, error) {
// 	sets := make([]*docPointers, len(children))

// 	// retrieve child IDs
// 	for i, child := range children {
// 		docIDs, err := child.mergeDocIDs(acceptDuplicates)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "retrieve doc ids of child %d", i)
// 		}

// 		sets[i] = docIDs
// 	}

// 	if checksumsIdentical(sets) {
// 		// all children are identical, no need to merge, simply return the first
// 		// set
// 		return sets[0], nil
// 	}

// 	if acceptDuplicates {
// 		return mergeOrAcceptDuplicates(sets)
// 	}

// 	// merge OR
// 	var checksums [][]byte
// 	found := map[uint64]uint64{} // map[id]count
// 	for _, set := range sets {
// 		for _, pointer := range set.docIDs {
// 			count := found[pointer]
// 			count++
// 			found[pointer] = count
// 		}
// 		checksums = append(checksums, set.checksum)
// 	}

// 	var out docPointers
// 	for id := range found {
// 		out.docIDs = append(out.docIDs, id)
// 	}

// 	out.checksum = combineChecksums(checksums, filters.OperatorOr)
// 	return &out, nil
// }

// func checksumsIdentical(sets []*docPointers) bool {
// 	if len(sets) == 0 {
// 		return false
// 	}

// 	if len(sets) == 1 {
// 		return true
// 	}

// 	lastChecksum := sets[0].checksum
// 	for _, set := range sets {
// 		if !bytes.Equal(set.checksum, lastChecksum) {
// 			return false
// 		}
// 	}

// 	return true
// }
