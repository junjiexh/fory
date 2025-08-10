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
	"hash/fnv"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/fory/go/fory/meta"
)

// TypeDefEncoder encodes Go types into TypeDef format
type TypeDefEncoder struct {
	metaStringEncoder *meta.Encoder
}

// NewTypeDefEncoder creates a new TypeDefEncoder
func NewTypeDefEncoder() *TypeDefEncoder {
	return &TypeDefEncoder{
		metaStringEncoder: meta.NewEncoder('$', '_'),
	}
}

// BuildTypeDef converts a Go type into a TypeDef
func (e *TypeDefEncoder) BuildTypeDef(goType reflect.Type) (*TypeDef, error) {
	if goType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("BuildTypeDef only supports struct types, got %v", goType.Kind())
	}

	fieldInfos, err := e.extractFieldInfos(goType)
	if err != nil {
		return nil, fmt.Errorf("failed to extract field infos: %w", err)
	}

	typeDef := NewTypeDef()
	typeDef.SetFieldInfos(fieldInfos)

	// Compute hash based on field information
	hash := e.computeTypeHash(fieldInfos)
	typeDef.SetHash(hash)

	// Encode the TypeDef into binary format
	encoded, err := e.encodeClassDef(typeDef)
	if err != nil {
		return nil, fmt.Errorf("failed to encode class definition: %w", err)
	}

	typeDef.SetEncoded(encoded)
	return typeDef, nil
}

// extractFieldInfos extracts field information from a struct type
func (e *TypeDefEncoder) extractFieldInfos(structType reflect.Type) ([]FieldInfo, error) {
	var fieldInfos []FieldInfo

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		fieldInfo, err := e.createFieldInfo(field)
		if err != nil {
			return nil, fmt.Errorf("failed to create field info for field %s: %w", field.Name, err)
		}

		fieldInfos = append(fieldInfos, fieldInfo)
	}

	// Sort fields by name for consistent ordering
	sort.Slice(fieldInfos, func(i, j int) bool {
		return fieldInfos[i].name < fieldInfos[j].name
	})

	return fieldInfos, nil
}

// createFieldInfo creates a FieldInfo from a reflect.StructField
func (e *TypeDefEncoder) createFieldInfo(field reflect.StructField) (FieldInfo, error) {
	fieldName := field.Name

	// Get field name encoding
	nameEncoding := e.metaStringEncoder.ComputeEncoding(fieldName)

	// Determine field type
	fieldType := GetTypeIdForGoType(field.Type)

	// Check nullable and reference tracking
	isNullable := nullable(field.Type)
	hasRefTracking := e.hasFieldRefTracking(field)

	// Create nested type info for complex types
	var nestedTypeInfo *NestedTypeInfo
	if fieldType == LIST || fieldType == MAP || fieldType == SET {
		nestedTypeInfo = e.createNestedTypeInfo(field.Type)
	}

	return FieldInfo{
		name:           fieldName,
		nameEncoding:   nameEncoding,
		fieldType:      fieldType,
		isNullable:     isNullable,
		hasRefTracking: hasRefTracking,
		nestedTypeInfo: nestedTypeInfo,
	}, nil
}

// hasFieldRefTracking determines if a field has reference tracking enabled
func (e *TypeDefEncoder) hasFieldRefTracking(field reflect.StructField) bool {
	// Check for ref tracking tag
	if tag, ok := field.Tag.Lookup("ref"); ok {
		return tag == "true"
	}

	// Enable ref tracking for reference types by default
	switch field.Type.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Interface:
		return true
	case reflect.Struct:
		return true
	default:
		return false
	}
}

// createNestedTypeInfo creates nested type information for collections
func (e *TypeDefEncoder) createNestedTypeInfo(fieldType reflect.Type) *NestedTypeInfo {
	switch fieldType.Kind() {
	case reflect.Slice:
		elemType := GetTypeIdForGoType(fieldType.Elem())
		return &NestedTypeInfo{
			valueType:           elemType,
			isValueNullable:     nullable(fieldType.Elem()),
			hasValueRefTracking: e.hasTypeRefTracking(fieldType.Elem()),
		}
	case reflect.Map:
		keyType := GetTypeIdForGoType(fieldType.Key())
		valueType := GetTypeIdForGoType(fieldType.Elem())
		return &NestedTypeInfo{
			keyType:             keyType,
			valueType:           valueType,
			isKeyNullable:       nullable(fieldType.Key()),
			isValueNullable:     nullable(fieldType.Elem()),
			hasKeyRefTracking:   e.hasTypeRefTracking(fieldType.Key()),
			hasValueRefTracking: e.hasTypeRefTracking(fieldType.Elem()),
		}
	default:
		return nil
	}
}

// hasTypeRefTracking checks if a type should have reference tracking
func (e *TypeDefEncoder) hasTypeRefTracking(goType reflect.Type) bool {
	switch goType.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Interface, reflect.Struct:
		return true
	default:
		return false
	}
}

// computeTypeHash computes a hash value for field infos
func (e *TypeDefEncoder) computeTypeHash(fieldInfos []FieldInfo) uint64 {
	h := fnv.New64a()

	// Hash the field count
	h.Write([]byte{byte(len(fieldInfos))})

	// Hash each field info
	for _, field := range fieldInfos {
		h.Write([]byte(field.name))
		h.Write([]byte{byte(field.fieldType)})
		if field.isNullable {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		if field.hasRefTracking {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	}

	return h.Sum64()
}

// encodeClassDef encodes a TypeDef into binary format according to the specification
func (e *TypeDefEncoder) encodeClassDef(typeDef *TypeDef) ([]byte, error) {
	buffer := NewByteBuffer(nil)

	// First, encode the metadata (header + fields)
	metaBuffer := NewByteBuffer(nil)

	// Write meta header
	if err := e.writeMetaHeader(metaBuffer, typeDef.GetFieldInfos()); err != nil {
		return nil, fmt.Errorf("failed to write meta header: %w", err)
	}

	// Write fields info
	if err := e.writeFieldsInfo(metaBuffer, typeDef.GetFieldInfos()); err != nil {
		return nil, fmt.Errorf("failed to write fields info: %w", err)
	}

	metaData := metaBuffer.GetByteSlice(0, metaBuffer.WriterIndex())
	metaSize := len(metaData)

	// Write global binary header
	if err := e.writeGlobalBinaryHeader(buffer, metaSize, typeDef.GetHash()); err != nil {
		return nil, fmt.Errorf("failed to write global binary header: %w", err)
	}

	// Write metadata
	buffer.WriteBinary(metaData)

	return buffer.GetByteSlice(0, buffer.WriterIndex()), nil
}

// writeGlobalBinaryHeader writes the 8-byte global binary header
func (e *TypeDefEncoder) writeGlobalBinaryHeader(buffer *ByteBuffer, metaSize int, hash uint64) error {
	var header uint64

	// Meta size (12 bits, lower bits)
	if metaSize < 4095 {
		header |= uint64(metaSize) & 0xFFF
	} else {
		header |= 0xFFF // Set to max value, actual size will follow
	}

	// Write fields meta flag (13th bit) - always true for now
	header |= 1 << 12

	// Compress flag (14th bit) - false for now
	// header |= 0 << 13

	// Hash (50 bits, upper bits)
	hashValue := hash & 0x3FFFFFFFFFFFF // Mask to 50 bits
	header |= hashValue << 14

	// Write the 8-byte header in little endian
	buffer.WriteInt64(int64(header))

	// If meta size >= 4095, write the additional size as varint
	if metaSize >= 4095 {
		buffer.WriteVarUint32(uint32(metaSize - 4095))
	}

	return nil
}

// writeMetaHeader writes the 1-byte meta header
func (e *TypeDefEncoder) writeMetaHeader(buffer *ByteBuffer, fieldInfos []FieldInfo) error {
	var header uint8

	numFields := len(fieldInfos)

	// Num fields (lower 5 bits)
	if numFields < 31 {
		header |= uint8(numFields) & 0x1F
	} else {
		header |= 0x1F // Set to max value, actual count will follow
	}

	// Registration method (6th bit) - 0 for ID registration, 1 for name registration
	// For now, assume name registration
	header |= 1 << 5

	// Reserved bits (7th and 8th bit) - leave as 0

	buffer.WriteByte_(header)

	// If num fields >= 31, write the additional count as varint
	if numFields >= 31 {
		buffer.WriteVarUint32(uint32(numFields - 31))
	}

	return nil
}

// writeFieldsInfo writes field information according to the specification
func (e *TypeDefEncoder) writeFieldsInfo(buffer *ByteBuffer, fieldInfos []FieldInfo) error {
	for _, field := range fieldInfos {
		if err := e.writeFieldInfo(buffer, field); err != nil {
			return fmt.Errorf("failed to write field info for field %s: %w", field.name, err)
		}
	}
	return nil
}

// writeFieldInfo writes a single field's information
func (e *TypeDefEncoder) writeFieldInfo(buffer *ByteBuffer, field FieldInfo) error {
	// Write field header
	if err := e.writeFieldHeader(buffer, field); err != nil {
		return fmt.Errorf("failed to write field header: %w", err)
	}

	// Write field type info
	if err := e.writeFieldTypeInfo(buffer, field); err != nil {
		return fmt.Errorf("failed to write field type info: %w", err)
	}

	// Write field name
	if err := e.writeFieldName(buffer, field); err != nil {
		return fmt.Errorf("failed to write field name: %w", err)
	}

	return nil
}

// writeFieldHeader writes the 1-byte field header
func (e *TypeDefEncoder) writeFieldHeader(buffer *ByteBuffer, field FieldInfo) error {
	var header uint8

	// Field name encoding (2 bits)
	header |= uint8(field.nameEncoding) & 0x03

	// Size of field name (4 bits) - store length - 1 (0-14 range)
	nameLen := len(field.name)
	if nameLen < 15 {
		header |= uint8((nameLen-1)&0x0F) << 2
	} else {
		header |= 0x0F << 2 // Max value, actual length will follow
	}

	// Nullability flag (7th bit)
	if field.isNullable {
		header |= 1 << 6
	}

	// Reference tracking flag (8th bit)
	if field.hasRefTracking {
		header |= 1 << 7
	}

	buffer.WriteByte_(header)

	// Write extended length if needed
	if nameLen >= 15 {
		buffer.WriteVarUint32(uint32(nameLen - 15))
	}

	return nil
}

// writeFieldTypeInfo writes field type information
func (e *TypeDefEncoder) writeFieldTypeInfo(buffer *ByteBuffer, field FieldInfo) error {
	// Write base type ID
	buffer.WriteByte_(byte(field.fieldType))

	// Write nested type info for complex types
	if field.nestedTypeInfo != nil {
		return e.writeNestedTypeInfo(buffer, field.fieldType, field.nestedTypeInfo)
	}

	return nil
}

// writeNestedTypeInfo writes nested type information for collections
func (e *TypeDefEncoder) writeNestedTypeInfo(buffer *ByteBuffer, parentType TypeId, nested *NestedTypeInfo) error {
	switch parentType {
	case LIST, SET:
		// Format: | nested type ID << 2 + nullable flag + ref tracking flag |
		var typeInfo uint8 = uint8(nested.valueType) << 2
		if nested.isValueNullable {
			typeInfo |= 1
		}
		if nested.hasValueRefTracking {
			typeInfo |= 2
		}
		buffer.WriteByte_(typeInfo)

	case MAP:
		// Format: | key type info | value type info |
		// Key type info
		var keyTypeInfo uint8 = uint8(nested.keyType) << 2
		if nested.isKeyNullable {
			keyTypeInfo |= 1
		}
		if nested.hasKeyRefTracking {
			keyTypeInfo |= 2
		}
		buffer.WriteByte_(keyTypeInfo)

		// Value type info
		var valueTypeInfo uint8 = uint8(nested.valueType) << 2
		if nested.isValueNullable {
			valueTypeInfo |= 1
		}
		if nested.hasValueRefTracking {
			valueTypeInfo |= 2
		}
		buffer.WriteByte_(valueTypeInfo)
	}

	return nil
}

// writeFieldName writes the field name using meta string encoding
func (e *TypeDefEncoder) writeFieldName(buffer *ByteBuffer, field FieldInfo) error {
	// Encode field name using meta string encoder
	metaStr, err := e.metaStringEncoder.Encode(field.name)
	if err != nil {
		return fmt.Errorf("failed to encode field name %s: %w", field.name, err)
	}

	// Write the encoded bytes
	encodedBytes := metaStr.GetEncodedBytes()
	if encodedBytes != nil {
		buffer.WriteBinary(encodedBytes)
	} else {
		// Empty field name case
		buffer.WriteBinary([]byte(field.name))
	}

	return nil
}

// GetFieldNamesByEncoding returns field names grouped by encoding type
func (e *TypeDefEncoder) GetFieldNamesByEncoding(fieldInfos []FieldInfo) map[meta.Encoding][]string {
	result := make(map[meta.Encoding][]string)

	for _, field := range fieldInfos {
		encoding := field.nameEncoding
		if _, exists := result[encoding]; !exists {
			result[encoding] = make([]string, 0)
		}
		result[encoding] = append(result[encoding], field.name)
	}

	return result
}

// ValidateTypeDef validates that a TypeDef is well-formed
func (e *TypeDefEncoder) ValidateTypeDef(typeDef *TypeDef) error {
	if typeDef == nil {
		return fmt.Errorf("TypeDef cannot be nil")
	}

	fieldInfos := typeDef.GetFieldInfos()
	if len(fieldInfos) == 0 {
		return fmt.Errorf("TypeDef must have at least one field")
	}

	// Check for duplicate field names
	nameMap := make(map[string]bool)
	for _, field := range fieldInfos {
		if field.name == "" {
			return fmt.Errorf("field name cannot be empty")
		}
		if nameMap[field.name] {
			return fmt.Errorf("duplicate field name: %s", field.name)
		}
		nameMap[field.name] = true

		// Validate field type
		if field.fieldType < 0 {
			return fmt.Errorf("invalid field type ID: %d", field.fieldType)
		}
	}

	return nil
}

// GetTypeDefStats returns statistics about a TypeDef
func (e *TypeDefEncoder) GetTypeDefStats(typeDef *TypeDef) map[string]interface{} {
	stats := make(map[string]interface{})

	fieldInfos := typeDef.GetFieldInfos()
	stats["fieldCount"] = len(fieldInfos)
	stats["encodedSize"] = len(typeDef.GetEncoded())
	stats["hash"] = fmt.Sprintf("0x%x", typeDef.GetHash())

	// Count fields by type
	typeCounts := make(map[TypeId]int)
	nullableCount := 0
	refTrackingCount := 0

	for _, field := range fieldInfos {
		typeCounts[field.fieldType]++
		if field.isNullable {
			nullableCount++
		}
		if field.hasRefTracking {
			refTrackingCount++
		}
	}

	stats["typeDistribution"] = typeCounts
	stats["nullableFieldCount"] = nullableCount
	stats["refTrackingFieldCount"] = refTrackingCount

	// Encoding statistics
	encodingCounts := e.GetFieldNamesByEncoding(fieldInfos)
	encodingStats := make(map[string]int)
	for encoding, names := range encodingCounts {
		var encodingName string
		switch encoding {
		case meta.UTF_8:
			encodingName = "UTF8"
		case meta.ALL_TO_LOWER_SPECIAL:
			encodingName = "ALL_TO_LOWER_SPECIAL"
		case meta.LOWER_UPPER_DIGIT_SPECIAL:
			encodingName = "LOWER_UPPER_DIGIT_SPECIAL"
		case meta.LOWER_SPECIAL:
			encodingName = "LOWER_SPECIAL"
		case meta.FIRST_TO_LOWER_SPECIAL:
			encodingName = "FIRST_TO_LOWER_SPECIAL"
		default:
			encodingName = fmt.Sprintf("UNKNOWN_%d", encoding)
		}
		encodingStats[encodingName] = len(names)
	}
	stats["encodingDistribution"] = encodingStats

	return stats
}

// DebugTypeDef returns a human-readable representation of a TypeDef
func (e *TypeDefEncoder) DebugTypeDef(typeDef *TypeDef) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("TypeDef(hash=0x%x, encodedSize=%d)\n",
		typeDef.GetHash(), len(typeDef.GetEncoded())))

	fieldInfos := typeDef.GetFieldInfos()
	sb.WriteString(fmt.Sprintf("Fields(%d):\n", len(fieldInfos)))

	for i, field := range fieldInfos {
		sb.WriteString(fmt.Sprintf("  [%d] %s: type=%d", i, field.name, field.fieldType))
		if field.isNullable {
			sb.WriteString(" nullable")
		}
		if field.hasRefTracking {
			sb.WriteString(" refTracking")
		}
		if field.nestedTypeInfo != nil {
			sb.WriteString(fmt.Sprintf(" nested(key=%d,val=%d)",
				field.nestedTypeInfo.keyType, field.nestedTypeInfo.valueType))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}
