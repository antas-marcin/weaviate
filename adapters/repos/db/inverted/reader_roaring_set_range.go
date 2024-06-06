//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"fmt"
	"math"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

type ReaderRoaringSetRange struct {
	value    uint64
	operator filters.Operator
	cursorFn func() lsmkv.CursorRoaringSetRange
}

func NewReaderRoaringSetRange(value uint64, operator filters.Operator,
	cursorFn func() lsmkv.CursorRoaringSetRange,
) *ReaderRoaringSetRange {
	return &ReaderRoaringSetRange{
		value:    value,
		operator: operator,
		cursorFn: cursorFn,
	}
}

func (r *ReaderRoaringSetRange) Read(ctx context.Context) (*sroar.Bitmap, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	switch r.operator {
	case filters.OperatorEqual:
		return r.equal(ctx)
	case filters.OperatorNotEqual:
		return r.notEqual(ctx)
	case filters.OperatorGreaterThan:
		return r.greaterThan(ctx)
	case filters.OperatorGreaterThanEqual:
		return r.greaterThanEqual(ctx)
	case filters.OperatorLessThan:
		return r.lessThan(ctx)
	case filters.OperatorLessThanEqual:
		return r.lessThanEqual(ctx)

	default:
		return nil, fmt.Errorf("operator %v not supported for strategy %q", r.operator.Name(), lsmkv.StrategyRoaringSetRange)
	}
}

func (r *ReaderRoaringSetRange) greaterThanEqual(ctx context.Context) (*sroar.Bitmap, error) {
	resultBM, cursor, ok, err := r.nonNullBMWithCursor(ctx)
	if !ok {
		return resultBM, err
	}

	// all values are >= 0
	if r.value == 0 {
		return resultBM, nil
	}

	return r.mergeGreaterThanEqual(ctx, resultBM, cursor, r.value)
}

func (r *ReaderRoaringSetRange) greaterThan(ctx context.Context) (*sroar.Bitmap, error) {
	// no value is > max uint64
	if r.value == math.MaxUint64 {
		return sroar.NewBitmap(), nil
	}

	resultBM, cursor, ok, err := r.nonNullBMWithCursor(ctx)
	if !ok {
		return resultBM, err
	}

	return r.mergeGreaterThanEqual(ctx, resultBM, cursor, r.value+1)
}

func (r *ReaderRoaringSetRange) lessThanEqual(ctx context.Context) (*sroar.Bitmap, error) {
	resultBM, cursor, ok, err := r.nonNullBMWithCursor(ctx)
	if !ok {
		return resultBM, err
	}

	// all values are <= max uint64
	if r.value == math.MaxUint64 {
		return resultBM, nil
	}

	greaterThanEqualBM, err := r.mergeGreaterThanEqual(ctx, resultBM.Clone(), cursor, r.value+1)
	if err != nil {
		return nil, err
	}
	resultBM.AndNot(greaterThanEqualBM)
	return resultBM, nil
}

func (r *ReaderRoaringSetRange) lessThan(ctx context.Context) (*sroar.Bitmap, error) {
	// no value is < 0
	if r.value == 0 {
		return sroar.NewBitmap(), nil
	}

	resultBM, cursor, ok, err := r.nonNullBMWithCursor(ctx)
	if !ok {
		return resultBM, err
	}

	greaterThanEqualBM, err := r.mergeGreaterThanEqual(ctx, resultBM.Clone(), cursor, r.value)
	if err != nil {
		return nil, err
	}
	resultBM.AndNot(greaterThanEqualBM)
	return resultBM, nil
}

func (r *ReaderRoaringSetRange) equal(ctx context.Context) (*sroar.Bitmap, error) {
	if r.value == 0 {
		return r.lessThanEqual(ctx)
	}
	if r.value == math.MaxUint64 {
		return r.greaterThanEqual(ctx)
	}

	resultBM, cursor, ok, err := r.nonNullBMWithCursor(ctx)
	if !ok {
		return resultBM, err
	}

	return r.mergeEqual(ctx, resultBM, cursor, r.value)
}

func (r *ReaderRoaringSetRange) notEqual(ctx context.Context) (*sroar.Bitmap, error) {
	if r.value == 0 {
		return r.greaterThan(ctx)
	}
	if r.value == math.MaxUint64 {
		return r.lessThan(ctx)
	}

	resultBM, cursor, ok, err := r.nonNullBMWithCursor(ctx)
	if !ok {
		return resultBM, err
	}

	equalBM, err := r.mergeEqual(ctx, resultBM.Clone(), cursor, r.value)
	if err != nil {
		return nil, err
	}
	resultBM.AndNot(equalBM)
	return resultBM, nil
}

func (r *ReaderRoaringSetRange) nonNullBMWithCursor(ctx context.Context) (*sroar.Bitmap, *noGapsCursor, bool, error) {
	cursor := &noGapsCursor{cursor: r.cursorFn()}
	_, nonNullBM, _ := cursor.first()

	// if non-null bm is nil or empty, no values are present
	if nonNullBM == nil || nonNullBM.IsEmpty() {
		cursor.close()
		return sroar.NewBitmap(), nil, false, nil
	}

	if ctx.Err() != nil {
		cursor.close()
		return nil, nil, false, ctx.Err()
	}

	return nonNullBM.Clone(), cursor, true, nil
}

func (r *ReaderRoaringSetRange) mergeGreaterThanEqual(ctx context.Context, resBM *sroar.Bitmap,
	cursor *noGapsCursor, value uint64,
) (*sroar.Bitmap, error) {
	defer cursor.close()

	for bit, bitBM, ok := cursor.next(); ok; bit, bitBM, ok = cursor.next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// And/Or handle properly nil bitmaps, so bitBM == nil is fine
		if value&(1<<(bit-1)) != 0 {
			resBM.And(bitBM)
		} else {
			resBM.Or(bitBM)
		}
	}

	return resBM, nil
}

func (r *ReaderRoaringSetRange) mergeEqual(ctx context.Context, resBM *sroar.Bitmap,
	cursor *noGapsCursor, value uint64,
) (*sroar.Bitmap, error) {
	defer cursor.close()

	resBM1 := resBM.Clone()
	value1 := value + 1
	for bit, bitBM, ok := cursor.next(); ok; bit, bitBM, ok = cursor.next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var b uint64 = 1 << (bit - 1)
		if value&b != 0 {
			resBM.And(bitBM)
		} else {
			resBM.Or(bitBM)
		}
		if value1&b != 0 {
			resBM1.And(bitBM)
		} else {
			resBM1.Or(bitBM)
		}
	}

	resBM.AndNot(resBM1)
	return resBM, nil
}

type noGapsCursor struct {
	cursor  lsmkv.CursorRoaringSetRange
	key     uint8
	started bool

	lastKey uint8
	lastVal *sroar.Bitmap
	lastOk  bool
}

func (c *noGapsCursor) first() (uint8, *sroar.Bitmap, bool) {
	c.started = true

	c.lastKey, c.lastVal, c.lastOk = c.cursor.First()

	c.key = 1
	if c.lastOk && c.lastKey == 0 {
		return 0, c.lastVal, true
	}
	return 0, nil, true
}

func (c *noGapsCursor) next() (uint8, *sroar.Bitmap, bool) {
	if !c.started {
		return c.first()
	}

	if c.key >= 65 {
		return 0, nil, false
	}

	for c.lastOk && c.lastKey < c.key {
		c.lastKey, c.lastVal, c.lastOk = c.cursor.Next()
	}

	key := c.key
	c.key++
	if c.lastOk && c.lastKey == key {
		return key, c.lastVal, true
	}
	return key, nil, true
}

func (c *noGapsCursor) close() {
	c.cursor.Close()
}
