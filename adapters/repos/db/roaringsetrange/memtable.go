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
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type Memtable struct {
	bitsAdditions map[uint8]*sroar.Bitmap
	nnAdditions   *sroar.Bitmap
	nnDeletions   *sroar.Bitmap
}

func NewMemtable() *Memtable {
	return &Memtable{
		bitsAdditions: make(map[uint8]*sroar.Bitmap),
		nnAdditions:   sroar.NewBitmap(),
		nnDeletions:   sroar.NewBitmap(),
	}
}

func (m *Memtable) Insert(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.nnAdditions.Set(v)
		m.nnDeletions.Set(v)
	}

	for bit := uint8(0); bit < 64; bit++ {
		_, ok := m.bitsAdditions[bit]

		if key&(1<<bit) == 0 {
			if ok {
				for _, v := range values {
					m.bitsAdditions[bit].Remove(v)
				}
			}
		} else {
			if !ok {
				m.bitsAdditions[bit] = sroar.NewBitmap()
			}
			for _, v := range values {
				m.bitsAdditions[bit].Set(v)
			}
		}
	}
}

func (m *Memtable) Delete(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.nnAdditions.Remove(v)
		m.nnDeletions.Set(v)
	}
	for bit := range m.bitsAdditions {
		for _, v := range values {
			m.bitsAdditions[bit].Remove(v)
		}
	}
}

func (m *Memtable) Nodes() []*MemtableNode {
	if m.nnAdditions.IsEmpty() && m.nnDeletions.IsEmpty() {
		return []*MemtableNode{}
	}

	nodes := make([]*MemtableNode, 1, 1+len(m.bitsAdditions))
	nodes[0] = &MemtableNode{
		Key:       0,
		Additions: roaringset.Condense(m.nnAdditions),
		Deletions: roaringset.Condense(m.nnDeletions),
	}

	bmEmpty := sroar.NewBitmap()
	l := 1
	for i := uint8(0); i < 64; i++ {
		if bitAdditions, ok := m.bitsAdditions[i]; ok && !bitAdditions.IsEmpty() {
			l++
			nodes = append(nodes, &MemtableNode{
				Key:       i + 1,
				Additions: roaringset.Condense(bitAdditions),
				Deletions: bmEmpty,
			})
		}
	}

	return nodes[:l]
}

type MemtableNode struct {
	Key       uint8
	Additions *sroar.Bitmap
	Deletions *sroar.Bitmap
}
