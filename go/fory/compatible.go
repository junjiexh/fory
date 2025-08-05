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
	"unicode"
	"unicode/utf8"
)

// CompatibleMode defines the compatibility mode for serialization
type CompatibleMode int8

const (
	STRICT_COMPATIBLE CompatibleMode = iota
	COMPATIBLE
)

// ClassSpec contains class identification info
type ClassSpec struct {
	TypeName    string
	PackagePath string
	TypeID      int32
	Hash        int64
}

// FieldType represents different field type categories
type FieldType interface {
	TypeID() int32
	Write(buf *ByteBuffer) error
	Read(buf *ByteBuffer) error
}

// RegisteredFieldType for registered/primitive types
type RegisteredFieldType struct {
	ID int32
}

func (r *RegisteredFieldType) TypeID() int32 { return r.ID }

func (r *RegisteredFieldType) Write(buf *ByteBuffer) error {
	buf.WriteInt32(r.ID)
	return nil
}

func (r *RegisteredFieldType) Read(buf *ByteBuffer) error {
	r.ID = buf.ReadInt32()
	return nil
}

// CollectionFieldType for collections with element type info
type CollectionFieldType struct {
	ID          int32
	ElementType FieldType
}

func (c *CollectionFieldType) TypeID() int32 { return c.ID }

func (c *CollectionFieldType) Write(buf *ByteBuffer) error {
	buf.WriteInt32(c.ID)
	return c.ElementType.Write(buf)
}

func (c *CollectionFieldType) Read(buf *ByteBuffer) error {
	c.ID = buf.ReadInt32()
	// ElementType needs to be created based on ID read next
	return nil
}

// MapFieldType for maps with key/value type info
type MapFieldType struct {
	ID        int32
	KeyType   FieldType
	ValueType FieldType
}

func (m *MapFieldType) TypeID() int32 { return m.ID }

func (m *MapFieldType) Write(buf *ByteBuffer) error {
	buf.WriteInt32(m.ID)
	if err := m.KeyType.Write(buf); err != nil {
		return err
	}
	return m.ValueType.Write(buf)
}

func (m *MapFieldType) Read(buf *ByteBuffer) error {
	m.ID = buf.ReadInt32()
	// KeyType and ValueType need to be created based on subsequent reads
	return nil
}

// CompatibleFieldInfo contains field metadata for compatibility
type CompatibleFieldInfo struct {
	Name         string
	FieldIndex   int
	Type         reflect.Type
	Hash         int64
	TypeID       int32
	FieldType    FieldType
	Referencable bool
	Serializer   Serializer
}

// ClassDef represents complete class definition for compatibility
type ClassDef struct {
	ClassSpec *ClassSpec
	Fields    []*CompatibleFieldInfo
	Hash      int64
	ClassID   int32
}

// Write serializes ClassDef to buffer
func (c *ClassDef) Write(buf *ByteBuffer) error {
	// Write class spec
	if err := c.writeClassSpec(buf); err != nil {
		return err
	}

	// Write fields count
	buf.WriteVarInt32(int32(len(c.Fields)))

	// Write each field
	for _, field := range c.Fields {
		if err := c.writeFieldInfo(buf, field); err != nil {
			return err
		}
	}

	return nil
}

// Read deserializes ClassDef from buffer
func (c *ClassDef) Read(buf *ByteBuffer) error {
	// Read class spec
	if err := c.readClassSpec(buf); err != nil {
		return err
	}

	// Read fields count
	fieldsCount := buf.ReadVarInt32()
	c.Fields = make([]*CompatibleFieldInfo, fieldsCount)

	// Read each field
	for i := int32(0); i < fieldsCount; i++ {
		field := &CompatibleFieldInfo{}
		if err := c.readFieldInfo(buf, field); err != nil {
			return err
		}
		c.Fields[i] = field
	}

	return nil
}

func (c *ClassDef) writeClassSpec(buf *ByteBuffer) error {
	// Write type name and package path using meta strings
	if err := writeCompatibleString(buf, c.ClassSpec.TypeName); err != nil {
		return err
	}
	if err := writeCompatibleString(buf, c.ClassSpec.PackagePath); err != nil {
		return err
	}
	buf.WriteInt32(c.ClassSpec.TypeID)
	buf.WriteInt64(c.ClassSpec.Hash)
	return nil
}

func (c *ClassDef) readClassSpec(buf *ByteBuffer) error {
	c.ClassSpec = &ClassSpec{}

	typeName, err := readCompatibleString(buf)
	if err != nil {
		return err
	}
	c.ClassSpec.TypeName = typeName

	packagePath, err := readCompatibleString(buf)
	if err != nil {
		return err
	}
	c.ClassSpec.PackagePath = packagePath

	c.ClassSpec.TypeID = buf.ReadInt32()
	c.ClassSpec.Hash = buf.ReadInt64()
	return nil
}

func (c *ClassDef) writeFieldInfo(buf *ByteBuffer, field *CompatibleFieldInfo) error {
	// Write field name
	if err := writeCompatibleString(buf, field.Name); err != nil {
		return err
	}

	// Write field hash and type info
	buf.WriteInt64(field.Hash)
	buf.WriteInt32(field.TypeID)

	// Write field type
	return field.FieldType.Write(buf)
}

func (c *ClassDef) readFieldInfo(buf *ByteBuffer, field *CompatibleFieldInfo) error {
	// Read field name
	name, err := readCompatibleString(buf)
	if err != nil {
		return err
	}
	field.Name = name

	// Read field hash and type info
	field.Hash = buf.ReadInt64()
	field.TypeID = buf.ReadInt32()

	// Create and read field type based on TypeID
	field.FieldType = createFieldType(field.TypeID)
	return field.FieldType.Read(buf)
}

// Helper functions for string serialization
func writeCompatibleString(buf *ByteBuffer, s string) error {
	data := []byte(s)
	buf.WriteVarInt32(int32(len(data)))
	buf.Write(data)
	return nil
}

func readCompatibleString(buf *ByteBuffer) (string, error) {
	length := buf.ReadVarInt32()
	data := make([]byte, length)
	_, err := buf.Read(data)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// createFieldType creates appropriate FieldType based on TypeID
func createFieldType(typeID int32) FieldType {
	switch {
	case typeID == LIST:
		return &CollectionFieldType{ID: typeID}
	case typeID == MAP:
		return &MapFieldType{ID: typeID}
	default:
		return &RegisteredFieldType{ID: typeID}
	}
}

// computeCompatibleFieldHash computes hash for a field using field name and type info
func computeCompatibleFieldHash(fieldName string, fieldType reflect.Type) int64 {
	h := fnv.New64a()
	h.Write([]byte(fieldName))
	h.Write([]byte(fieldType.String()))
	return int64(h.Sum64())
}

// computeClassHash computes hash for entire class definition
func computeClassHash(spec *ClassSpec, fields []*CompatibleFieldInfo) int64 {
	h := fnv.New64a()

	// Handle nil spec case
	if spec != nil {
		h.Write([]byte(spec.TypeName))
		h.Write([]byte(spec.PackagePath))
	}

	// Sort fields by name for consistent hashing
	if len(fields) > 0 {
		sortedFields := make([]*CompatibleFieldInfo, len(fields))
		copy(sortedFields, fields)
		sort.Slice(sortedFields, func(i, j int) bool {
			return sortedFields[i].Name < sortedFields[j].Name
		})

		for _, field := range sortedFields {
			h.Write([]byte(field.Name))
			hashBytes := make([]byte, 8)
			for i := 0; i < 8; i++ {
				hashBytes[i] = byte(field.Hash >> (8 * i))
			}
			h.Write(hashBytes)
		}
	}

	return int64(h.Sum64())
}

// createCompatibleFieldInfos creates field info for compatible serialization
func createCompatibleFieldInfos(f *Fory, type_ reflect.Type) ([]*CompatibleFieldInfo, error) {
	var fields []*CompatibleFieldInfo

	for i := 0; i < type_.NumField(); i++ {
		field := type_.Field(i)

		// Skip unexported fields
		firstRune, _ := utf8.DecodeRuneInString(field.Name)
		if unicode.IsLower(firstRune) {
			continue
		}

		// Create field info
		fieldInfo := &CompatibleFieldInfo{
			Name:       SnakeCase(field.Name),
			FieldIndex: i,
			Type:       field.Type,
			Hash:       computeCompatibleFieldHash(field.Name, field.Type),
		}

		// Get type ID and serializer
		if typeInfo, err := f.typeResolver.getTypeInfo(reflect.Zero(field.Type), true); err == nil {
			fieldInfo.TypeID = typeInfo.TypeID
			fieldInfo.Serializer = typeInfo.Serializer
		} else {
			// Fallback for basic types
			fieldInfo.TypeID = getBasicTypeID(field.Type)
		}

		// Create field type
		fieldInfo.FieldType = createFieldTypeFromReflectType(field.Type, fieldInfo.TypeID)
		fieldInfo.Referencable = nullable(field.Type)

		fields = append(fields, fieldInfo)
	}

	// Sort fields by hash for consistent serialization order
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Hash < fields[j].Hash
	})

	return fields, nil
}

// createFieldTypeFromReflectType creates FieldType from reflect.Type
func createFieldTypeFromReflectType(t reflect.Type, typeID int32) FieldType {
	switch t.Kind() {
	case reflect.Slice:
		elemTypeID := getBasicTypeID(t.Elem())
		return &CollectionFieldType{
			ID:          typeID,
			ElementType: &RegisteredFieldType{ID: elemTypeID},
		}
	case reflect.Map:
		keyTypeID := getBasicTypeID(t.Key())
		valueTypeID := getBasicTypeID(t.Elem())
		return &MapFieldType{
			ID:        typeID,
			KeyType:   &RegisteredFieldType{ID: keyTypeID},
			ValueType: &RegisteredFieldType{ID: valueTypeID},
		}
	default:
		return &RegisteredFieldType{ID: typeID}
	}
}

// getBasicTypeID returns TypeID for basic Go types
func getBasicTypeID(t reflect.Type) int32 {
	switch t.Kind() {
	case reflect.Bool:
		return BOOL
	case reflect.Int8:
		return INT8
	case reflect.Int16:
		return INT16
	case reflect.Int32:
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
		return LIST
	case reflect.Map:
		return MAP
	case reflect.Struct:
		return NAMED_STRUCT
	case reflect.Ptr:
		if t.Elem().Kind() == reflect.Struct {
			return FORY_TYPE_TAG
		}
		return getBasicTypeID(t.Elem())
	default:
		return 0 // Unknown type
	}
}

// FieldEncoding represents different field encoding strategies
type FieldEncoding int8

const (
	EMBED_TYPES_4       FieldEncoding = iota // Short field names (≤4 chars) with embedded class ID
	EMBED_TYPES_9                            // Medium field names (≤9 chars) with embedded class ID
	EMBED_TYPES_HASH                         // Long field names with MurmurHash3
	SEPARATE_TYPES_HASH                      // Complex types with separate type information
)

// FieldResolver handles field-level compatibility logic
type FieldResolver struct {
	encodingStrategy FieldEncoding
}

// NewFieldResolver creates a new FieldResolver
func NewFieldResolver() *FieldResolver {
	return &FieldResolver{
		encodingStrategy: EMBED_TYPES_HASH, // Default to hash-based encoding
	}
}

// EncodeFieldInfo encodes field information for serialization
func (fr *FieldResolver) EncodeFieldInfo(field *CompatibleFieldInfo) ([]byte, error) {
	buf := NewByteBuffer(nil)

	switch fr.encodingStrategy {
	case EMBED_TYPES_4:
		return fr.encodeTypes4(buf, field)
	case EMBED_TYPES_9:
		return fr.encodeTypes9(buf, field)
	case EMBED_TYPES_HASH:
		return fr.encodeTypesHash(buf, field)
	case SEPARATE_TYPES_HASH:
		return fr.encodeSeparateTypesHash(buf, field)
	default:
		return fr.encodeTypesHash(buf, field)
	}
}

// encodeTypes4 encodes field info for short field names (≤4 chars)
func (fr *FieldResolver) encodeTypes4(buf *ByteBuffer, field *CompatibleFieldInfo) ([]byte, error) {
	if len(field.Name) > 4 {
		return nil, fmt.Errorf("field name '%s' too long for EMBED_TYPES_4 encoding", field.Name)
	}

	// Pack field name into 4 bytes
	nameBytes := make([]byte, 4)
	copy(nameBytes, []byte(field.Name))
	buf.Write(nameBytes)

	// Embed type ID
	buf.WriteInt32(field.TypeID)

	return buf.GetByteSlice(0, buf.writerIndex), nil
}

// encodeTypes9 encodes field info for medium field names (≤9 chars)
func (fr *FieldResolver) encodeTypes9(buf *ByteBuffer, field *CompatibleFieldInfo) ([]byte, error) {
	if len(field.Name) > 9 {
		return nil, fmt.Errorf("field name '%s' too long for EMBED_TYPES_9 encoding", field.Name)
	}

	// Pack field name into 9 bytes
	nameBytes := make([]byte, 9)
	copy(nameBytes, []byte(field.Name))
	buf.Write(nameBytes)

	// Embed type ID
	buf.WriteInt32(field.TypeID)

	return buf.GetByteSlice(0, buf.writerIndex), nil
}

// encodeTypesHash encodes field info using MurmurHash3
func (fr *FieldResolver) encodeTypesHash(buf *ByteBuffer, field *CompatibleFieldInfo) ([]byte, error) {
	// Use field hash directly (already computed with MurmurHash3-like algorithm)
	buf.WriteInt64(field.Hash)
	buf.WriteInt32(field.TypeID)

	return buf.GetByteSlice(0, buf.writerIndex), nil
}

// encodeSeparateTypesHash encodes complex types with separate type information
func (fr *FieldResolver) encodeSeparateTypesHash(buf *ByteBuffer, field *CompatibleFieldInfo) ([]byte, error) {
	// Write field hash
	buf.WriteInt64(field.Hash)

	// Write type ID
	buf.WriteInt32(field.TypeID)

	// Write additional type information
	if err := field.FieldType.Write(buf); err != nil {
		return nil, err
	}

	return buf.GetByteSlice(0, buf.writerIndex), nil
}

// DecodeFieldInfo decodes field information during deserialization
func (fr *FieldResolver) DecodeFieldInfo(buf *ByteBuffer, encoding FieldEncoding) (*CompatibleFieldInfo, error) {
	field := &CompatibleFieldInfo{}

	switch encoding {
	case EMBED_TYPES_4:
		return fr.decodeTypes4(buf, field)
	case EMBED_TYPES_9:
		return fr.decodeTypes9(buf, field)
	case EMBED_TYPES_HASH:
		return fr.decodeTypesHash(buf, field)
	case SEPARATE_TYPES_HASH:
		return fr.decodeSeparateTypesHash(buf, field)
	default:
		return fr.decodeTypesHash(buf, field)
	}
}

// decodeTypes4 decodes field info for short field names
func (fr *FieldResolver) decodeTypes4(buf *ByteBuffer, field *CompatibleFieldInfo) (*CompatibleFieldInfo, error) {
	nameBytes := make([]byte, 4)
	_, err := buf.Read(nameBytes)
	if err != nil {
		return nil, err
	}

	// Extract field name (trim null bytes)
	field.Name = string(nameBytes)
	field.Name = trimNullBytes(field.Name)

	field.TypeID = buf.ReadInt32()
	field.FieldType = createFieldType(field.TypeID)

	return field, nil
}

// decodeTypes9 decodes field info for medium field names
func (fr *FieldResolver) decodeTypes9(buf *ByteBuffer, field *CompatibleFieldInfo) (*CompatibleFieldInfo, error) {
	nameBytes := make([]byte, 9)
	_, err := buf.Read(nameBytes)
	if err != nil {
		return nil, err
	}

	// Extract field name (trim null bytes)
	field.Name = string(nameBytes)
	field.Name = trimNullBytes(field.Name)

	field.TypeID = buf.ReadInt32()
	field.FieldType = createFieldType(field.TypeID)

	return field, nil
}

// decodeTypesHash decodes field info using hash
func (fr *FieldResolver) decodeTypesHash(buf *ByteBuffer, field *CompatibleFieldInfo) (*CompatibleFieldInfo, error) {
	field.Hash = buf.ReadInt64()
	field.TypeID = buf.ReadInt32()
	field.FieldType = createFieldType(field.TypeID)

	return field, nil
}

// decodeSeparateTypesHash decodes complex types with separate type information
func (fr *FieldResolver) decodeSeparateTypesHash(buf *ByteBuffer, field *CompatibleFieldInfo) (*CompatibleFieldInfo, error) {
	field.Hash = buf.ReadInt64()
	field.TypeID = buf.ReadInt32()
	field.FieldType = createFieldType(field.TypeID)

	// Read additional type information
	if err := field.FieldType.Read(buf); err != nil {
		return nil, err
	}

	return field, nil
}

// trimNullBytes removes trailing null bytes from string
func trimNullBytes(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != 0 {
			return s[:i+1]
		}
	}
	return ""
}

// CompareFields compares two field sets for compatibility
func (fr *FieldResolver) CompareFields(writerFields, readerFields []*CompatibleFieldInfo) (FieldCompatibility, error) {
	compatibility := FieldCompatibility{
		CommonFields:   make([]*FieldMapping, 0),
		AddedFields:    make([]*CompatibleFieldInfo, 0),
		RemovedFields:  make([]*CompatibleFieldInfo, 0),
		ModifiedFields: make([]*FieldMapping, 0),
	}

	// Create hash maps for efficient lookup
	writerFieldMap := make(map[int64]*CompatibleFieldInfo)
	readerFieldMap := make(map[int64]*CompatibleFieldInfo)

	for _, field := range writerFields {
		writerFieldMap[field.Hash] = field
	}

	for _, field := range readerFields {
		readerFieldMap[field.Hash] = field
	}

	// Find common and modified fields
	for hash, writerField := range writerFieldMap {
		if readerField, exists := readerFieldMap[hash]; exists {
			if writerField.TypeID == readerField.TypeID {
				// Field exists and type matches
				compatibility.CommonFields = append(compatibility.CommonFields, &FieldMapping{
					WriterField: writerField,
					ReaderField: readerField,
				})
			} else {
				// Field exists but type changed
				compatibility.ModifiedFields = append(compatibility.ModifiedFields, &FieldMapping{
					WriterField: writerField,
					ReaderField: readerField,
				})
			}
		} else {
			// Field added in writer
			compatibility.AddedFields = append(compatibility.AddedFields, writerField)
		}
	}

	// Find removed fields
	for hash, readerField := range readerFieldMap {
		if _, exists := writerFieldMap[hash]; !exists {
			compatibility.RemovedFields = append(compatibility.RemovedFields, readerField)
		}
	}

	return compatibility, nil
}

// FieldCompatibility represents the compatibility analysis between two field sets
type FieldCompatibility struct {
	CommonFields   []*FieldMapping        // Fields present in both with same type
	AddedFields    []*CompatibleFieldInfo // Fields added in writer
	RemovedFields  []*CompatibleFieldInfo // Fields removed from reader
	ModifiedFields []*FieldMapping        // Fields with type changes
}

// FieldMapping maps writer field to reader field
type FieldMapping struct {
	WriterField *CompatibleFieldInfo
	ReaderField *CompatibleFieldInfo
}

// CanSkipField determines if a field can be safely skipped during deserialization
func (fr *FieldResolver) CanSkipField(field *CompatibleFieldInfo) bool {
	// Most fields can be skipped, except for fields that affect object structure
	switch field.TypeID {
	case FORY_BUFFER, FORY_ARROW_RECORD_BATCH, FORY_ARROW_TABLE:
		return false // These may have out-of-band data
	default:
		return true
	}
}

// END_TAG marker for field serialization
const END_TAG = int64(-1)

// compatibleSerializer handles forward/backward compatible serialization
type compatibleSerializer struct {
	typeTag       string
	type_         reflect.Type
	fieldsInfo    []*CompatibleFieldInfo
	classID       int32
	needWriteDef  bool
	fieldResolver *FieldResolver
}

// NewCompatibleSerializer creates a new compatible serializer
func NewCompatibleSerializer(typeTag string, type_ reflect.Type) *compatibleSerializer {
	return &compatibleSerializer{
		typeTag:       typeTag,
		type_:         type_,
		needWriteDef:  true,
		fieldResolver: NewFieldResolver(),
	}
}

func (s *compatibleSerializer) TypeId() TypeId {
	return NAMED_COMPATIBLE_STRUCT
}

func (s *compatibleSerializer) NeedWriteRef() bool {
	return true
}

func (s *compatibleSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	// Initialize fields info if not done
	if s.fieldsInfo == nil {
		if fieldsInfo, err := createCompatibleFieldInfos(f, s.type_); err != nil {
			return fmt.Errorf("failed to create compatible field infos: %w", err)
		} else {
			s.fieldsInfo = fieldsInfo
		}
	}

	// Get or register class ID
	if s.classID == 0 {
		s.classID = f.metaContext.RegisterClass(s.type_)
	}

	// Write class ID reference
	buf.WriteInt32(s.classID)

	// Check if we need to write class definition
	if s.needWriteClassDef(f) {
		if err := s.writeClassDef(f, buf); err != nil {
			return fmt.Errorf("failed to write class definition: %w", err)
		}
		s.needWriteDef = false
	} else {
		// Write flag indicating no class definition follows
		buf.WriteByte(0)
	}

	// Write fields in hash order for position independence
	// For now, simplified approach - just write the field values directly
	for _, fieldInfo := range s.fieldsInfo {
		fieldValue := value.Field(fieldInfo.FieldIndex)
		if err := s.writeFieldValue(f, buf, fieldValue, fieldInfo); err != nil {
			return fmt.Errorf("failed to write field value for %s: %w", fieldInfo.Name, err)
		}
	}

	return nil
}

func (s *compatibleSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	// Handle pointer types
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			value.Set(reflect.New(type_.Elem()))
		}
		value = value.Elem()
	}

	// Read class ID
	classID := buf.ReadInt32()

	// Get class definition
	classDef, exists := f.metaContext.GetClassDef(classID)
	if !exists {
		// Check if class definition follows (flag byte)
		hasClassDef := buf.ReadByte_()
		if hasClassDef == 1 {
			// Read class definition
			classDef = &ClassDef{}
			if err := classDef.Read(buf); err != nil {
				return fmt.Errorf("failed to read class definition: %w", err)
			}
			f.metaContext.SetClassDef(classID, classDef)
		} else {
			return fmt.Errorf("class definition for ID %d not found and not provided", classID)
		}
	}

	// Initialize our field info if needed
	if s.fieldsInfo == nil {
		if fieldsInfo, err := createCompatibleFieldInfos(f, s.type_); err != nil {
			return fmt.Errorf("failed to create compatible field infos: %w", err)
		} else {
			s.fieldsInfo = fieldsInfo
		}
	}

	// For now, simplified approach - just read field values in same order
	// This matches what we write in the Write method
	// TODO: Implement proper field compatibility checking later
	for _, fieldInfo := range s.fieldsInfo {
		fieldValue := value.Field(fieldInfo.FieldIndex)
		if err := s.readFieldValue(f, buf, fieldValue, fieldInfo, fieldInfo.TypeID); err != nil {
			return fmt.Errorf("failed to read field %s: %w", fieldInfo.Name, err)
		}
	}

	return nil
}

func (s *compatibleSerializer) needWriteClassDef(f *Fory) bool {
	if !s.needWriteDef {
		return false
	}

	// Check if class is already shared with peer
	_, exists := f.metaContext.GetClassID(s.type_)
	return !exists || s.needWriteDef
}

func (s *compatibleSerializer) writeClassDef(f *Fory, buf *ByteBuffer) error {
	// Create class spec
	classSpec := &ClassSpec{
		TypeName:    s.type_.Name(),
		PackagePath: s.type_.PkgPath(),
		TypeID:      int32(NAMED_COMPATIBLE_STRUCT),
		Hash:        computeClassHash(nil, s.fieldsInfo), // Will be computed in computeClassHash
	}

	// Create class def
	classDef := &ClassDef{
		ClassSpec: classSpec,
		Fields:    s.fieldsInfo,
		ClassID:   s.classID,
		Hash:      classSpec.Hash,
	}

	// Recompute hash with actual class spec
	classDef.Hash = computeClassHash(classSpec, s.fieldsInfo)
	classSpec.Hash = classDef.Hash

	// Write class definition flag
	buf.WriteByte(1) // Indicates class definition follows

	// Write class definition
	return classDef.Write(buf)
}

func (s *compatibleSerializer) writeFieldHeader(buf *ByteBuffer, fieldInfo *CompatibleFieldInfo) error {
	// Write field hash and type ID for position independence
	buf.WriteInt64(fieldInfo.Hash)
	buf.WriteInt32(fieldInfo.TypeID)
	return nil
}

func (s *compatibleSerializer) writeFieldValue(f *Fory, buf *ByteBuffer, fieldValue reflect.Value, fieldInfo *CompatibleFieldInfo) error {
	if fieldInfo.Serializer != nil {
		// Use specific serializer
		if fieldInfo.Referencable {
			return f.writeReferencableBySerializer(buf, fieldValue, fieldInfo.Serializer)
		} else {
			return f.writeNonReferencableBySerializer(buf, fieldValue, fieldInfo.Serializer)
		}
	} else {
		// Use generic serialization
		return f.WriteReferencable(buf, fieldValue)
	}
}

func (s *compatibleSerializer) readFieldValue(f *Fory, buf *ByteBuffer, fieldValue reflect.Value, fieldInfo *CompatibleFieldInfo, writerTypeID int32) error {
	// Check type compatibility
	if writerTypeID != fieldInfo.TypeID {
		// Types don't match - attempt conversion or skip
		return s.convertFieldValue(f, buf, fieldValue, fieldInfo, writerTypeID)
	}

	if fieldInfo.Serializer != nil {
		// Use specific serializer
		if fieldInfo.Referencable {
			return f.readReferencableBySerializer(buf, fieldValue, fieldInfo.Serializer)
		} else {
			return fieldInfo.Serializer.Read(f, buf, fieldInfo.Type, fieldValue)
		}
	} else {
		// Use generic deserialization
		return f.ReadReferencable(buf, fieldValue)
	}
}

func (s *compatibleSerializer) convertFieldValue(f *Fory, buf *ByteBuffer, fieldValue reflect.Value, fieldInfo *CompatibleFieldInfo, writerTypeID int32) error {
	// For now, skip incompatible fields
	// TODO: Implement type conversion logic for compatible types (e.g., int32 -> int64)
	return s.skipField(f, buf, writerTypeID)
}

func (s *compatibleSerializer) skipField(f *Fory, buf *ByteBuffer, typeID int32) error {
	// Create a temporary value to read into and discard
	switch typeID {
	case BOOL:
		buf.ReadBool()
	case INT8:
		buf.ReadInt8()
	case INT16:
		buf.ReadInt16()
	case INT32:
		buf.ReadInt32()
	case INT64:
		buf.ReadInt64()
	case FLOAT:
		buf.ReadFloat32()
	case DOUBLE:
		buf.ReadFloat64()
	case STRING:
		// Read string length and skip data
		length := buf.ReadVarInt32()
		buf.Skip(int(length))
	case LIST:
		// Skip list by reading length and skipping elements
		return s.skipList(f, buf)
	case MAP:
		// Skip map by reading length and skipping key-value pairs
		return s.skipMap(f, buf)
	default:
		// For complex types, we need to read the full object to skip it properly
		// This is a simplified approach - in practice, we'd need more sophisticated skipping
		return f.ReadReferencable(buf, reflect.ValueOf(new(interface{})).Elem())
	}
	return nil
}

func (s *compatibleSerializer) skipList(f *Fory, buf *ByteBuffer) error {
	// Read list length
	length := buf.ReadVarInt32()

	// Read element type ID
	elemTypeID := buf.ReadInt32()

	// Skip each element
	for i := int32(0); i < length; i++ {
		if err := s.skipField(f, buf, elemTypeID); err != nil {
			return err
		}
	}
	return nil
}

func (s *compatibleSerializer) skipMap(f *Fory, buf *ByteBuffer) error {
	// Read map length
	length := buf.ReadVarInt32()

	// Read key and value type IDs
	keyTypeID := buf.ReadInt32()
	valueTypeID := buf.ReadInt32()

	// Skip each key-value pair
	for i := int32(0); i < length; i++ {
		if err := s.skipField(f, buf, keyTypeID); err != nil {
			return err
		}
		if err := s.skipField(f, buf, valueTypeID); err != nil {
			return err
		}
	}
	return nil
}

func (s *compatibleSerializer) setDefaultValue(fieldValue reflect.Value, fieldType reflect.Type) {
	if !fieldValue.CanSet() {
		return
	}

	// Set zero value for the field type
	fieldValue.Set(reflect.Zero(fieldType))
}

// ptrToCompatibleStructSerializer serializes pointer to compatible struct
type ptrToCompatibleStructSerializer struct {
	type_ reflect.Type
	compatibleSerializer
}

func NewPtrToCompatibleStructSerializer(typeTag string, type_ reflect.Type) *ptrToCompatibleStructSerializer {
	return &ptrToCompatibleStructSerializer{
		type_:                type_,
		compatibleSerializer: *NewCompatibleSerializer(typeTag, type_.Elem()),
	}
}

func (s *ptrToCompatibleStructSerializer) TypeId() TypeId {
	return FORY_TYPE_TAG
}

func (s *ptrToCompatibleStructSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	return s.compatibleSerializer.Write(f, buf, value.Elem())
}

func (s *ptrToCompatibleStructSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	newValue := reflect.New(type_.Elem())
	value.Set(newValue)
	elem := newValue.Elem()
	f.refResolver.Reference(newValue)
	return s.compatibleSerializer.Read(f, buf, type_.Elem(), elem)
}
