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
	"reflect"

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
	name           string
	nameEncoding   meta.Encoding
	fieldType      TypeId
	isNullable     bool
	hasRefTracking bool
	nestedTypeInfo *NestedTypeInfo
}

// NestedTypeInfo contains information about nested types (for collections, etc.)
type NestedTypeInfo struct {
	keyType             TypeId
	valueType           TypeId
	isKeyNullable       bool
	isValueNullable     bool
	hasKeyRefTracking   bool
	hasValueRefTracking bool
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

// GetHash returns the computed hash value
func (td *TypeDef) GetHash() uint64 {
	return td.hash
}

// SetFieldInfos sets the field information
func (td *TypeDef) SetFieldInfos(fieldInfos []FieldInfo) {
	td.fieldInfos = fieldInfos
}

// SetEncoded sets the encoded bytes
func (td *TypeDef) SetEncoded(encoded []byte) {
	td.encoded = encoded
}

// SetHash sets the hash value
func (td *TypeDef) SetHash(hash uint64) {
	td.hash = hash
}

// IsPolymorphicType checks if a type is polymorphic (needs separate metadata)
func IsPolymorphicType(typeID TypeId) bool {
	switch typeID {
	case STRUCT, NAMED_STRUCT, EXTENSION, NAMED_EXT:
		return true
	default:
		return false
	}
}

// IsMonomorphicType checks if a type is monomorphic (type info not written during value serialization)
func IsMonomorphicType(typeID TypeId) bool {
	return !IsPolymorphicType(typeID)
}

// GetTypeIdForGoType maps Go types to Fory TypeId
func GetTypeIdForGoType(goType reflect.Type) TypeId {
	switch goType.Kind() {
	case reflect.Bool:
		return BOOL
	case reflect.Int8:
		return INT8
	case reflect.Int16:
		return INT16
	case reflect.Int32, reflect.Int:
		return INT32
	case reflect.Int64:
		return INT64
	case reflect.Float32:
		return FLOAT
	case reflect.Float64:
		return DOUBLE
	case reflect.String:
		return STRING
	case reflect.Slice:
		if goType.Elem().Kind() == reflect.Uint8 { // []byte
			return BINARY
		}
		return LIST
	case reflect.Array:
		// Map to appropriate array type
		elem := goType.Elem()
		switch elem.Kind() {
		case reflect.Bool:
			return BOOL_ARRAY
		case reflect.Int8:
			return INT8_ARRAY
		case reflect.Int16:
			return INT16_ARRAY
		case reflect.Int32, reflect.Int:
			return INT32_ARRAY
		case reflect.Int64:
			return INT64_ARRAY
		case reflect.Float32:
			return FLOAT32_ARRAY
		case reflect.Float64:
			return FLOAT64_ARRAY
		default:
			return ARRAY
		}
	case reflect.Map:
		return MAP
	case reflect.Struct:
		return NAMED_STRUCT
	case reflect.Ptr:
		// For pointers, get the type of the pointed-to element
		return GetTypeIdForGoType(goType.Elem())
	case reflect.Interface:
		return FORY_TYPE_TAG
	default:
		return FORY_TYPE_TAG // Unknown type
	}
}
