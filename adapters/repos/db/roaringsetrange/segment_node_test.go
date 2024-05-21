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

func TestSegmentNode_HappyPath(t *testing.T) {
	additions := roaringset.NewBitmap(1, 2, 3, 4, 6)
	deletions := roaringset.NewBitmap(5, 7)
	key := uint8(123)

	sn, err := NewSegmentNode(key, additions, deletions)
	require.Nil(t, err)

	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	snBuf := NewSegmentNodeFromBuffer(buf)
	assert.Equal(t, snBuf.Len(), uint64(len(buf)))

	newAdditions := snBuf.Additions()
	assert.True(t, newAdditions.Contains(4))
	assert.False(t, newAdditions.Contains(5))
	newDeletions := snBuf.Deletions()
	assert.False(t, newDeletions.Contains(4))
	assert.True(t, newDeletions.Contains(5))
	assert.Equal(t, uint8(123), snBuf.Key())
}

func TestSegmentNode_InitializingFromBufferTooLarge(t *testing.T) {
	additions := roaringset.NewBitmap(1, 2, 3, 4, 6)
	deletions := roaringset.NewBitmap(5, 7)
	key := uint8(123)

	sn, err := NewSegmentNode(key, additions, deletions)
	require.Nil(t, err)

	buf := sn.ToBuffer()
	assert.Equal(t, sn.Len(), uint64(len(buf)))

	bufTooLarge := make([]byte, 3*len(buf))
	copy(bufTooLarge, buf)

	snBuf := NewSegmentNodeFromBuffer(bufTooLarge)
	// assert that the buffer self reports the useful length, not the length of
	// the initialization buffer
	assert.Equal(t, snBuf.Len(), uint64(len(buf)))
	// assert that ToBuffer() returns a buffer that is no longer than the useful
	// length
	assert.Equal(t, len(buf), len(snBuf.ToBuffer()))
}
