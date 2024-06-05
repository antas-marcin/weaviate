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

// Read a row using the specified ReadFn. If RowReader was created with
// keysOnly==true, the values argument in the readFn will always be nil on all
// requests involving cursors
func (r *ReaderRoaringSetRange) Read(ctx context.Context) (*sroar.Bitmap, error) {
	switch r.operator {
	// case filters.OperatorEqual:
	// 	return r.equal(ctx)
	// case filters.OperatorNotEqual:
	// 	return r.notEqual(ctx)
	// case filters.OperatorGreaterThan:
	// 	return r.greaterThan(ctx)
	// case filters.OperatorGreaterThanEqual:
	// 	return r.greaterThanEqual(ctx)
	// case filters.OperatorLessThan:
	// 	return r.lessThan(ctx)
	// case filters.OperatorLessThanEqual:
	// 	return r.lessThanEqual(ctx)

	default:
		return nil, fmt.Errorf("operator %v not supported for strategy %q", r.operator, lsmkv.StrategyRoaringSetRange)
	}
}

type rsrCursor struct {
	cursor  lsmkv.CursorRoaringSetRange
	key     uint8
	started bool

	lastKey uint8
	lastVal *sroar.Bitmap
	lastOk  bool
}

func (c *rsrCursor) first() (uint8, *sroar.Bitmap, bool) {
	c.started = true

	c.lastKey, c.lastVal, c.lastOk = c.cursor.First()

	c.key = 1
	if c.lastOk && c.lastKey == 0 {
		return 0, c.lastVal, true
	}
	return 0, nil, true
}

func (c *rsrCursor) next() (uint8, *sroar.Bitmap, bool) {
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

// func newRsrCursor(cursor *lsmkv.CursorRoaringSetRange) *rsrCursor {
// 	return &rsrCursor{
// 		cursor:  cursor,
// 		key : 0,
// 		lastKey: 0,
// 	}
// }

// func (c *rsrCursor) First() (uint8, *sroar.Bitmap, bool) {
// 	c.lastKey = 0
// 	k, v, ok := c.cur.First()

// 	if ok {
// 		if k == c.lastKey {

// 		}
// 	}

// }

// func (c *rsrCursor) Next() (uint8, *sroar.Bitmap, bool) {

// 	k, v, ok := c.cur.First()

// }

// // greaterThan reads from the specified value to the end. The first row is only
// // included if allowEqual==true, otherwise it starts with the next one
// func (r *ReaderRoaringSetRange) greaterThanEqual(ctx context.Context) (*sroar.Bitmap, error) {
// 	cursor := r.cursorFn()
// 	defer cursor.Close()

// 	// 0 - 64

// 	k, v, ok := cursor.First()

// 	for k, v := c.Seek(rr.value); k != nil; k, v = c.Next() {
// 		if err := ctx.Err(); err != nil {
// 			return err
// 		}

// 		if bytes.Equal(k, rr.value) && !allowEqual {
// 			continue
// 		}

// 		if continueReading, err := readFn(k, v); err != nil {
// 			return err
// 		} else if !continueReading {
// 			break
// 		}
// 	}

// 	return nil
// }

// func (rr *RowReaderRoaringSet) equal(ctx context.Context) error {
// 	v, err := rr.equalHelper(ctx)
// 	if err != nil {
// 		return err
// 	}

// 	_, err = readFn(rr.value, v)
// 	return err
// }

// func (rr *RowReaderRoaringSet) notEqual(ctx context.Context,
// 	readFn ReadFn,
// ) error {
// 	v, err := rr.equalHelper(ctx)
// 	if err != nil {
// 		return err
// 	}

// 	inverted := rr.bitmapFactory.GetBitmap()
// 	inverted.AndNot(v)
// 	_, err = readFn(rr.value, inverted)
// 	return err
// }

// // greaterThan reads from the specified value to the end. The first row is only
// // included if allowEqual==true, otherwise it starts with the next one
// func (rr *RowReaderRoaringSet) greaterThan(ctx context.Context,
// 	readFn ReadFn, allowEqual bool,
// ) error {
// 	c := rr.newCursor()
// 	defer c.Close()

// 	for k, v := c.Seek(rr.value); k != nil; k, v = c.Next() {
// 		if err := ctx.Err(); err != nil {
// 			return err
// 		}

// 		if bytes.Equal(k, rr.value) && !allowEqual {
// 			continue
// 		}

// 		if continueReading, err := readFn(k, v); err != nil {
// 			return err
// 		} else if !continueReading {
// 			break
// 		}
// 	}

// 	return nil
// }

// // lessThan reads from the very begging to the specified  value. The last
// // matching row is only included if allowEqual==true, otherwise it ends one
// // prior to that.
// func (rr *RowReaderRoaringSet) lessThan(ctx context.Context,
// 	readFn ReadFn, allowEqual bool,
// ) error {
// 	c := rr.newCursor()
// 	defer c.Close()

// 	for k, v := c.First(); k != nil && bytes.Compare(k, rr.value) < 1; k, v = c.Next() {
// 		if err := ctx.Err(); err != nil {
// 			return err
// 		}

// 		if bytes.Equal(k, rr.value) && !allowEqual {
// 			continue
// 		}

// 		if continueReading, err := readFn(k, v); err != nil {
// 			return err
// 		} else if !continueReading {
// 			break
// 		}
// 	}

// 	return nil
// }

// func (rr *RowReaderRoaringSet) like(ctx context.Context,
// 	readFn ReadFn,
// ) error {
// 	like, err := parseLikeRegexp(rr.value)
// 	if err != nil {
// 		return fmt.Errorf("parse like value: %w", err)
// 	}

// 	c := rr.newCursor()
// 	defer c.Close()

// 	var (
// 		initialK   []byte
// 		initialV   *sroar.Bitmap
// 		likeMinLen int
// 	)

// 	if like.optimizable {
// 		initialK, initialV = c.Seek(like.min)
// 		likeMinLen = len(like.min)
// 	} else {
// 		initialK, initialV = c.First()
// 	}

// 	for k, v := initialK, initialV; k != nil; k, v = c.Next() {
// 		if err := ctx.Err(); err != nil {
// 			return err
// 		}

// 		if like.optimizable {
// 			// if the query is optimizable, i.e. it doesn't start with a wildcard, we
// 			// can abort once we've moved past the point where the fixed characters
// 			// no longer match
// 			if len(k) < likeMinLen {
// 				break
// 			}
// 			if bytes.Compare(like.min, k[:likeMinLen]) == -1 {
// 				break
// 			}
// 		}

// 		if !like.regexp.Match(k) {
// 			continue
// 		}

// 		if continueReading, err := readFn(k, v); err != nil {
// 			return err
// 		} else if !continueReading {
// 			break
// 		}
// 	}

// 	return nil
// }

// // equalHelper exists, because the Equal and NotEqual operators share this functionality
// func (rr *RowReaderRoaringSet) equalHelper(ctx context.Context) (*sroar.Bitmap, error) {
// 	if err := ctx.Err(); err != nil {
// 		return nil, err
// 	}

// 	return rr.getter(rr.value)
// }
