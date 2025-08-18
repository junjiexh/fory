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
	"fmt"
	"github.com/spaolacci/murmur3"
	"reflect"
)

const (
	SmallNumFieldsThreshold = 31
	FieldNameSizeThreshold  = 15
	META_SIZE_MASK          = 0xFFF
	COMPRESS_META_FLAG      = 0b1 << 13
	HAS_FIELDS_META_FLAG    = 0b1 << 12
	NUM_HASH_BITS           = 50
	REGISTER_BY_NAME_FLAG   = 0b1 << 5
)

// BuildTypeDef converts a Go type into a TypeDef
func BuildTypeDef(fory *Fory, value reflect.Value) (*TypeDef, error) {
	if value.Kind() != reflect.Struct {
		return nil, fmt.Errorf("BuildTypeDef only supports struct types, got %v", value.Kind())
	}

	fieldInfos, err := extractFieldInfos(fory.typeResolver, fory.refResolver, value)
	if err != nil {
		return nil, fmt.Errorf("failed to extract field infos: %w", err)
	}

	typeDef := NewTypeDef()
	typeDef.SetFieldInfos(fieldInfos)

	// Encode the TypeDef into binary format
	encoded, err := encodeTypeDef(fory.typeResolver, value, fieldInfos)
	if err != nil {
		return nil, fmt.Errorf("failed to encode class definition: %w", err)
	}

	typeDef.SetEncoded(encoded)
	return typeDef, nil
}

// extractFieldInfos extracts field information from a struct value
func extractFieldInfos(typeResolver *typeResolver, refResolver *RefResolver, structValue reflect.Value) ([]FieldInfo, error) {
	var fieldInfos []FieldInfo

	typ := structValue.Type()
	for i := 0; i < typ.NumField(); i++ {
		fieldValue := structValue.Field(i)

		var fieldInfo FieldInfo
		fieldType := fieldValue.Type()
		fieldName := fieldType.Name()

		nameEncoding := typeResolver.typeNameEncoder.ComputeEncoding(fieldName)
		typeId, err := typeResolver.getTypeInfo(fieldValue, true)
		if err != nil {
			return nil, err
		}

		fieldInfo = FieldInfo{
			name:         fieldName,
			nameEncoding: nameEncoding,
			typeId:       TypeId(typeId.TypeID),
			isNullable:   nullable(fieldType),
			refTracking:  refResolver.refTracking,
		}
		fieldInfos = append(fieldInfos, fieldInfo)
	}
	return fieldInfos, nil
}

// encodeTypeDef encodes a TypeDef into binary format according to the specification
func encodeTypeDef(typeResolver *typeResolver, value reflect.Value, fieldInfos []FieldInfo) ([]byte, error) {
	buffer := NewByteBuffer(nil)

	metaBuffer := NewByteBuffer(nil)

	// Write meta header
	if err := writeMetaHeader(typeResolver, metaBuffer, value, fieldInfos); err != nil {
		return nil, fmt.Errorf("failed to write meta header: %w", err)
	}

	// Write fields info
	if err := writeFieldsInfo(typeResolver, metaBuffer, fieldInfos); err != nil {
		return nil, fmt.Errorf("failed to write fields info: %w", err)
	}

	// Write global binary header
	result, err := prependGlobalHeader(buffer, false, true)
	if err != nil {
		return nil, fmt.Errorf("failed to write global binary header: %w", err)
	}

	return result.GetByteSlice(0, result.WriterIndex()), nil
}

// prependGlobalHeader writes the 8-byte global header
func prependGlobalHeader(buffer *ByteBuffer, isCompressed bool, hasFieldsMeta bool) (*ByteBuffer, error) {
	var header uint64
	metaSize := buffer.WriterIndex()

	// Hash (50 bits, upper bits)
	hashValue := murmur3.Sum64WithSeed(buffer.GetByteSlice(0, metaSize), 47)
	header |= hashValue << (64 - NUM_HASH_BITS)

	// Write fields meta flag (13th bit)
	if hasFieldsMeta {
		header |= HAS_FIELDS_META_FLAG
	}

	// Compress flag (14th bit)
	if isCompressed {
		header |= COMPRESS_META_FLAG
	}

	// Meta size (12 bits, lower bits)
	if metaSize < META_SIZE_MASK {
		header |= uint64(metaSize) & 0xFFF
	} else {
		header |= 0xFFF // Set to max value, actual size will follow
	}

	result := NewByteBuffer(make([]byte, metaSize+8))
	result.WriteInt64(int64(header))

	// If meta size >= 4095, write the additional size as varint
	if metaSize >= META_SIZE_MASK {
		result.WriteVarUint32(uint32(metaSize - META_SIZE_MASK))
	}
	result.WriteBinary(buffer.GetByteSlice(0, metaSize))

	return result, nil
}

// writeMetaHeader writes the 1-byte meta header
func writeMetaHeader(typeResolver *typeResolver, buffer *ByteBuffer, value reflect.Value, fieldInfos []FieldInfo) error {
	offset := buffer.writerIndex
	if err := buffer.WriteByte(0xFF); err != nil {
		return err
	}
	header := len(fieldInfos)
	if header > SmallNumFieldsThreshold {
		header = SmallNumFieldsThreshold
		buffer.WriteVarUint32(uint32(len(fieldInfos) - SmallNumFieldsThreshold))
	}

	info, err := typeResolver.getTypeInfo(value, true)
	if err != nil {
		return fmt.Errorf("failed to get type info for value %v: %w", value, err)
	}

	if !IsNamespacedType(TypeId(info.TypeID)) {
		buffer.WriteVarUint32(uint32(info.TypeID))
	} else {
		header |= REGISTER_BY_NAME_FLAG
		if err := typeResolver.metaStringResolver.WriteMetaStringBytes(buffer, info.PkgPathBytes); err != nil {
			return err
		}
		if err := typeResolver.metaStringResolver.WriteMetaStringBytes(buffer, info.NameBytes); err != nil {
			return err
		}
	}

	buffer.PutUint8(offset, uint8(header))
	return nil
}

// writeFieldsInfo writes field information according to the specification
// |   field info: variable bytes    | variable bytes  | ... |
// +---------------------------------+-----------------+-----+
// | header + type info + field name | next field info | ... |
func writeFieldsInfo(typeResolver *typeResolver, buffer *ByteBuffer, fieldInfos []FieldInfo) error {
	for _, field := range fieldInfos {
		if err := writeFieldInfo(typeResolver, buffer, field); err != nil {
			return fmt.Errorf("failed to write field info for field %s: %w", field.name, err)
		}
	}
	return nil
}

// writeFieldInfo writes a single field's information
func writeFieldInfo(typeResolver *typeResolver, buffer *ByteBuffer, field FieldInfo) error {
	// Write field header
	// 2 bits field name encoding + 4 bits size + nullability flag + ref tracking flag
	offset := buffer.writerIndex
	if err := buffer.WriteByte(0xFF); err != nil {
		return err
	}
	var header uint8
	if field.refTracking {
		header = 1
	}
	if field.isNullable {
		header |= 0b10
	}
	header |= uint8(field.nameEncoding) & 0x03
	metaString, err := typeResolver.typeNameEncoder.Encode(field.name)
	if err != nil {
		return err
	}
	nameLen := len(metaString.GetEncodedBytes())
	if nameLen < FieldNameSizeThreshold {
		header |= uint8((nameLen-1)&0x0F) << 2
	} else {
		header |= 0x0F << 2 // Max value, actual length will follow
		buffer.WriteVarUint32(uint32(nameLen - FieldNameSizeThreshold))
	}
	buffer.PutUint8(offset, header)
	// Write field type ID
	buffer.WriteVarUint32Small7(uint32(field.typeId))
	return nil
}
