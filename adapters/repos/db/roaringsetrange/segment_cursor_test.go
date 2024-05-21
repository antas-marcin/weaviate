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

package roaringsetrange

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestSegmentCursor(t *testing.T) {
	seg := createDummySegment(t, 5)

	t.Run("starting from beginning", func(t *testing.T) {
		c := NewSegmentCursor(seg)
		key, layer, ok := c.First()
		require.True(t, ok)
		assert.Equal(t, uint8(0), key)
		assert.True(t, layer.Additions.Contains(0))
		assert.True(t, layer.Additions.Contains(1))
		assert.True(t, layer.Deletions.Contains(2))
		assert.True(t, layer.Deletions.Contains(3))
	})

	t.Run("starting from beginning, page through all", func(t *testing.T) {
		c := NewSegmentCursor(seg)
		it := uint64(0)
		for key, layer, ok := c.First(); ok; key, layer, ok = c.Next() {
			assert.Equal(t, uint8(it), key)
			assert.True(t, layer.Additions.Contains(it*4))
			assert.True(t, layer.Additions.Contains(it*4+1))
			assert.True(t, layer.Deletions.Contains(it*4+2))
			assert.True(t, layer.Deletions.Contains(it*4+3))
			it++
		}

		assert.Equal(t, uint64(5), it)
	})
}

func createDummySegment(t *testing.T, count uint64) []byte {
	out := []byte{}

	for i := uint64(0); i < count; i++ {
		key := uint8(i)
		add := roaringset.NewBitmap(i*4, i*4+1)
		del := roaringset.NewBitmap(i*4+2, i*4+3)
		sn, err := NewSegmentNode(key, add, del)
		require.Nil(t, err)

		out = append(out, sn.ToBuffer()...)
	}

	return out
}
