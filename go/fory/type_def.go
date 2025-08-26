// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package fory

import (
	"github.com/apache/fory/go/fory/meta"
)

const (
	META_SIZE_MASK       = 0xFFF
	COMPRESS_META_FLAG   = 0b1 << 13
	HAS_FIELDS_META_FLAG = 0b1 << 12
	NUM_HASH_BITS        = 50
)

// TypeDef represents a transportable value object containing class structure information
type TypeDef struct {
	fieldInfos []FieldInfo
	encoded    []byte
	hash       uint64
}

// FieldInfo contains information about a single field in a struct
type FieldInfo struct {
	name         string
	nameEncoding meta.Encoding
	typeId       TypeId
	isNullable   bool
	refTracking  bool
}

// NewTypeDef creates a new TypeDef instance
func NewTypeDef() *TypeDef {
	return &TypeDef{
		fieldInfos: make([]FieldInfo, 0),
		encoded:    nil,
		hash:       0,
	}
}

func (td *TypeDef) writeTypeDef(buffer *ByteBuffer) {
	buffer.WriteBinary(td.encoded)
}

func skipTypeDef(buffer *ByteBuffer, header int64) {
	sz := int(header & META_SIZE_MASK)
	if sz == META_SIZE_MASK {
		sz += int(buffer.ReadVarUint32())
	}
	buffer.IncreaseReaderIndex(sz)
}

func readTypeDefs(fory *Fory, buffer *ByteBuffer) (*TypeDef, error) {
	return decodeTypeDef(*fory.typeResolver, buffer, buffer.ReadInt64())
}
