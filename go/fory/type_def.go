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

// GetFieldInfos returns the field information
func (td *TypeDef) GetFieldInfos() []FieldInfo {
	return td.fieldInfos
}

// GetEncoded returns the encoded bytes
func (td *TypeDef) GetEncoded() []byte {
	return td.encoded
}

// SetFieldInfos sets the field information
func (td *TypeDef) SetFieldInfos(fieldInfos []FieldInfo) {
	td.fieldInfos = fieldInfos
}

// SetEncoded sets the encoded bytes
func (td *TypeDef) SetEncoded(encoded []byte) {
	td.encoded = encoded
}
