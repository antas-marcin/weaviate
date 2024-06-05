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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestRsrCursor(t *testing.T) {
	t.Run("empty CursorRoaringSetRange", func(t *testing.T) {
		c := &rsrCursor{cursor: newFakeCursorRoaringSetRange(map[uint64]uint64{})}

		k, v, ok := c.first()
		require.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.Nil(t, v)

		for i := uint8(1); i < 65; i++ {
			k, v, ok = c.next()
			require.Equal(t, i, k)
			require.True(t, ok)
			assert.Nil(t, v)
		}

		k, v, ok = c.next()
		require.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("non-empty CursorRoaringSetRange", func(t *testing.T) {
		c := &rsrCursor{cursor: newFakeCursorRoaringSetRange(map[uint64]uint64{
			113: 13, // 1101
			213: 13, // 1101
			15:  5,  // 0101
			25:  5,  // 0101
			10:  0,  // 0000
			20:  0,  // 0000
		})}

		k, v, ok := c.first()
		require.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, v.ToArray())

		expected := map[uint8][]uint64{
			1: {15, 25, 113, 213},
			3: {15, 25, 113, 213},
			4: {113, 213},
		}

		for i := uint8(1); i < 65; i++ {
			k, v, ok := c.next()
			require.Equal(t, i, k)
			require.True(t, ok)

			if expectedV, ok := expected[i]; ok {
				require.NotNil(t, v)
				assert.ElementsMatch(t, expectedV, v.ToArray())
			} else {
				assert.Nil(t, v)
			}
		}

		k, v, ok = c.next()
		require.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})
}

func TestFakeCursorRoaringSetRange(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		c := newFakeCursorRoaringSetRange(map[uint64]uint64{})

		k, v, ok := c.First()
		assert.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("non-empty", func(t *testing.T) {
		c := newFakeCursorRoaringSetRange(map[uint64]uint64{
			113: 13, // 1101
			213: 13, // 1101
			15:  5,  // 0101
			25:  5,  // 0101
			10:  0,  // 0000
			20:  0,  // 0000
		})

		k, v, ok := c.First()
		assert.Equal(t, uint8(0), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{10, 20, 15, 25, 113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(1), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(3), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{15, 25, 113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(4), k)
		require.True(t, ok)
		assert.ElementsMatch(t, []uint64{113, 213}, v.ToArray())

		k, v, ok = c.Next()
		assert.Equal(t, uint8(0), k)
		require.False(t, ok)
		assert.Nil(t, v)
	})
}

type fakeCursorRoaringSetRange struct {
	bitmaps map[uint8]*sroar.Bitmap
	bits    []uint8
	pos     int
}

func newFakeCursorRoaringSetRange(docId2Val map[uint64]uint64) *fakeCursorRoaringSetRange {
	bitmaps := make(map[uint8]*sroar.Bitmap, 65)

	for docId, val := range docId2Val {
		if bitmaps[0] == nil {
			bitmaps[0] = sroar.NewBitmap()
		}
		bitmaps[0].Set(docId)

		for i := uint8(0); i < 64; i++ {
			if val&(1<<i) != 0 {
				if bitmaps[i+1] == nil {
					bitmaps[i+1] = sroar.NewBitmap()
				}
				bitmaps[i+1].Set(docId)
			}
		}
	}

	bits := make([]uint8, 0, len(bitmaps))
	for bit := range bitmaps {
		bits = append(bits, bit)
	}
	sort.Slice(bits, func(i, j int) bool { return bits[i] < bits[j] })

	return &fakeCursorRoaringSetRange{
		bitmaps: bitmaps,
		bits:    bits,
		pos:     0,
	}
}

func (c *fakeCursorRoaringSetRange) First() (uint8, *sroar.Bitmap, bool) {
	c.pos = 0
	return c.Next()
}

func (c *fakeCursorRoaringSetRange) Next() (uint8, *sroar.Bitmap, bool) {
	if c.pos >= len(c.bits) {
		return 0, nil, false
	}

	bit := c.bits[c.pos]
	c.pos++
	return bit, c.bitmaps[bit], true
}

func (c *fakeCursorRoaringSetRange) Close() {}
