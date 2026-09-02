//go:build repro

// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestReproRejectReservedSchemaFieldIDs(t *testing.T) {
	current := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	planned := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2147483448, Name: "reserved", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := reproTable(t, current, 2)
	plan, state := reproSchemaPlanAndState(t, planned, current)
	var updateDiags diag.Diagnostics
	updates := (&icebergTableResource{}).calculateSchemaUpdates(context.Background(), &plan, &state, tbl, &updateDiags)

	if !updateDiags.HasError() {
		t.Fatalf("expected a field ID above 2147483447 to be rejected before commit, got %d updates", len(updates))
	}
}

func TestReproRejectDroppedSchemaFieldIDReuse(t *testing.T) {
	historical := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "dropped", Type: iceberg.PrimitiveTypes.String},
	)
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	planned := iceberg.NewSchema(2,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "replacement", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := reproTable(t, historical, 2)
	metadata, err := table.UpdateTableMetadata(tbl.Metadata(), []table.Update{
		table.NewAddSchemaUpdate(current),
		table.NewSetCurrentSchemaUpdate(1),
	}, "")
	if err != nil {
		t.Fatalf("drop field from current schema: %v", err)
	}
	tbl = table.New([]string{"db", "table"}, metadata, "", nil, nil)
	plan, state := reproSchemaPlanAndState(t, planned, current)
	var updateDiags diag.Diagnostics
	updates := (&icebergTableResource{}).calculateSchemaUpdates(context.Background(), &plan, &state, tbl, &updateDiags)

	if !updateDiags.HasError() {
		t.Fatalf("expected reuse of historical field ID 2 to be rejected before commit, got %d updates", len(updates))
	}
}

func TestReproRejectReparentedFieldIDs(t *testing.T) {
	tests := []struct {
		name    string
		current *iceberg.Schema
		planned *iceberg.Schema
	}{
		{
			name: "struct field",
			current: iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "left", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "child", Type: iceberg.PrimitiveTypes.String},
				}}},
				iceberg.NestedField{ID: 2, Name: "right", Type: &iceberg.StructType{}},
			),
			planned: iceberg.NewSchema(2,
				iceberg.NestedField{ID: 1, Name: "left", Type: &iceberg.StructType{}},
				iceberg.NestedField{ID: 2, Name: "right", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "child", Type: iceberg.PrimitiveTypes.String},
				}}},
			),
		},
		{
			name: "list element",
			current: iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "left", Type: &iceberg.ListType{ElementID: 3, Element: iceberg.PrimitiveTypes.String}},
				iceberg.NestedField{ID: 2, Name: "right", Type: &iceberg.ListType{ElementID: 4, Element: iceberg.PrimitiveTypes.String}},
			),
			planned: iceberg.NewSchema(2,
				iceberg.NestedField{ID: 1, Name: "left", Type: &iceberg.ListType{ElementID: 4, Element: iceberg.PrimitiveTypes.String}},
				iceberg.NestedField{ID: 2, Name: "right", Type: &iceberg.ListType{ElementID: 3, Element: iceberg.PrimitiveTypes.String}},
			),
		},
		{
			name: "map key and value",
			current: iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "left", Type: &iceberg.MapType{KeyID: 3, KeyType: iceberg.PrimitiveTypes.String, ValueID: 4, ValueType: iceberg.PrimitiveTypes.Int64}},
				iceberg.NestedField{ID: 2, Name: "right", Type: &iceberg.MapType{KeyID: 5, KeyType: iceberg.PrimitiveTypes.String, ValueID: 6, ValueType: iceberg.PrimitiveTypes.Int64}},
			),
			planned: iceberg.NewSchema(2,
				iceberg.NestedField{ID: 1, Name: "left", Type: &iceberg.MapType{KeyID: 5, KeyType: iceberg.PrimitiveTypes.String, ValueID: 6, ValueType: iceberg.PrimitiveTypes.Int64}},
				iceberg.NestedField{ID: 2, Name: "right", Type: &iceberg.MapType{KeyID: 3, KeyType: iceberg.PrimitiveTypes.String, ValueID: 4, ValueType: iceberg.PrimitiveTypes.Int64}},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateSchemaEvolution(tt.current, tt.planned); err == nil {
				t.Fatal("expected moving an existing field ID to a different parent to be rejected")
			}
		})
	}
}

func TestReproRejectHistoricalPartitionFieldIDReuse(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	historicalSpec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "old_identity",
		Transform: iceberg.IdentityTransform{},
	})
	metadata, err := table.NewMetadata(schema, &historicalSpec, table.UnsortedSortOrder, "s3://warehouse/table", nil)
	if err != nil {
		t.Fatalf("create table metadata: %v", err)
	}
	unpartitioned := iceberg.NewPartitionSpec()
	metadata, err = table.UpdateTableMetadata(metadata, []table.Update{
		table.NewAddPartitionSpecUpdate(&unpartitioned, false),
		table.NewSetDefaultSpecUpdate(-1),
	}, "")
	if err != nil {
		t.Fatalf("replace partition spec: %v", err)
	}
	tbl := table.New([]string{"db", "table"}, metadata, "", nil, nil)

	plannedSpec := icebergTablePartitionSpec{
		Fields: []icebergTablePartitionField{
			{
				SourceIDs: []int64{1},
				FieldID:   types.Int64Value(1000),
				Name:      "reassigned_bucket",
				Transform: "bucket[16]",
			},
		},
	}
	plannedSpecValue, conversionDiags := types.ObjectValueFrom(context.Background(), icebergTablePartitionSpec{}.AttrTypes(), plannedSpec)
	if conversionDiags.HasError() {
		t.Fatalf("create Terraform partition spec: %v", conversionDiags.Errors())
	}
	plan := icebergTableResourceModel{PartitionSpec: plannedSpecValue}
	var updateDiags diag.Diagnostics
	updates := (&icebergTableResource{}).calculatePartitionUpdates(context.Background(), &plan, tbl, &updateDiags)

	if !updateDiags.HasError() {
		t.Fatalf("expected historical partition field ID reuse to be rejected, got %d updates", len(updates))
	}
}

func TestReproCompleteOptimisticConcurrencyRequirements(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})
	metadata, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, "s3://warehouse/table", nil)
	if err != nil {
		t.Fatalf("create table metadata: %v", err)
	}

	// Mirrors icebergTableResource.Update in resource_table.go, which currently
	// sends only AssertTableUUID as an optimistic-concurrency requirement.
	requirements := []table.Requirement{
		table.AssertTableUUID(metadata.TableUUID()),
	}
	tests := []struct {
		name    string
		updates []table.Update
	}{
		{
			name: "concurrent schema update",
			updates: []table.Update{
				table.NewAddSchemaUpdate(iceberg.NewSchema(1,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 2, Name: "concurrent", Type: iceberg.PrimitiveTypes.String},
				)),
				table.NewSetCurrentSchemaUpdate(1),
			},
		},
		{
			name: "concurrent partition update",
			updates: []table.Update{
				table.NewAddPartitionSpecUpdate(reproPartitionSpec(1, 1001, "concurrent_bucket", iceberg.BucketTransform{NumBuckets: 16}), false),
				table.NewSetDefaultSpecUpdate(-1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			concurrent, err := table.UpdateTableMetadata(metadata, tt.updates, "")
			if err != nil {
				t.Fatalf("apply %s: %v", tt.name, err)
			}
			for _, requirement := range requirements {
				if err := requirement.Validate(concurrent); err != nil {
					return
				}
			}
			t.Fatal("expected optimistic-concurrency requirements to reject stale table metadata")
		})
	}
}

func reproPartitionSpec(specID, fieldID int, name string, transform iceberg.Transform) *iceberg.PartitionSpec {
	spec := iceberg.NewPartitionSpecID(specID, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   fieldID,
		Name:      name,
		Transform: transform,
	})

	return &spec
}

func reproTable(t *testing.T, schema *iceberg.Schema, formatVersion int) *table.Table {
	t.Helper()
	properties := iceberg.Properties{"format-version": fmt.Sprint(formatVersion)}
	metadata, err := table.NewMetadata(schema, nil, table.UnsortedSortOrder, "s3://warehouse/table", properties)
	if err != nil {
		t.Fatalf("create format-v%d table metadata: %v", formatVersion, err)
	}

	return table.New([]string{"db", "table"}, metadata, "", nil, nil)
}

func reproSchemaPlanAndState(t *testing.T, planned, current *iceberg.Schema) (icebergTableResourceModel, icebergTableResourceModel) {
	t.Helper()
	toTerraform := func(schema *iceberg.Schema) types.Object {
		var model icebergTableSchema
		if err := model.FromIceberg(schema); err != nil {
			t.Fatalf("convert Iceberg schema to Terraform model: %v", err)
		}
		value, conversionDiags := types.ObjectValueFrom(context.Background(), icebergTableSchema{}.AttrTypes(), model)
		if conversionDiags.HasError() {
			t.Fatalf("convert Terraform schema model to object: %v", conversionDiags.Errors())
		}

		return value
	}

	return icebergTableResourceModel{Schema: toTerraform(planned)}, icebergTableResourceModel{Schema: toTerraform(current)}
}

func TestReproPreserveIdentifierFieldIDs(t *testing.T) {
	original := iceberg.NewSchemaWithIdentifiers(7, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)

	var model icebergTableSchema
	if err := model.FromIceberg(original); err != nil {
		t.Fatalf("convert schema to Terraform model: %v", err)
	}
	roundTripped, err := model.ToIceberg()
	if err != nil {
		t.Fatalf("convert Terraform model to schema: %v", err)
	}

	if !slices.Equal(roundTripped.IdentifierFieldIDs, original.IdentifierFieldIDs) {
		t.Fatalf("identifier field IDs changed from %v to %v", original.IdentifierFieldIDs, roundTripped.IdentifierFieldIDs)
	}
}

func TestReproRejectRequiredFieldWithoutDefaults(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	planned := iceberg.NewSchema(2,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "required_without_defaults", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	tbl := reproTable(t, current, 3)
	plan, state := reproSchemaPlanAndState(t, planned, current)
	var updateDiags diag.Diagnostics
	updates := (&icebergTableResource{}).calculateSchemaUpdates(context.Background(), &plan, &state, tbl, &updateDiags)

	if !updateDiags.HasError() {
		t.Fatalf("expected a required field added without initial and write defaults to be rejected, got %d updates", len(updates))
	}
}

func TestReproReadNestedCollectionChildTypes(t *testing.T) {
	tests := []struct {
		name      string
		fieldType iceberg.Type
	}{
		{
			name: "list of structs",
			fieldType: &iceberg.ListType{ElementID: 2, Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 3, Name: "nested", Type: iceberg.PrimitiveTypes.Int64},
			}}},
		},
		{
			name: "map with struct values",
			fieldType: &iceberg.MapType{KeyID: 2, KeyType: iceberg.PrimitiveTypes.String, ValueID: 3, ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 4, Name: "nested", Type: iceberg.PrimitiveTypes.Int64},
			}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "collection", Type: tt.fieldType},
			)
			var model icebergTableSchema
			if err := model.FromIceberg(schema); err != nil {
				t.Fatalf("expected nested collection child type to be readable: %v", err)
			}
		})
	}
}

func TestReproReadFiveNestedStructLevels(t *testing.T) {
	var nested iceberg.Type = iceberg.PrimitiveTypes.String
	for id := 6; id >= 2; id-- {
		nested = &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: id, Name: "level", Type: nested},
		}}
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "root", Type: nested},
	)

	var model icebergTableSchema
	if err := model.FromIceberg(schema); err != nil {
		t.Fatalf("convert nested schema to Terraform model: %v", err)
	}
	_, diags := types.ObjectValueFrom(context.Background(), icebergTableSchema{}.AttrTypes(), model)
	if diags.HasError() {
		t.Fatalf("expected five nested struct levels to fit the Terraform schema: %v", diags.Errors())
	}
}

func TestReproPreserveMultiSourcePartitionField(t *testing.T) {
	var spec iceberg.PartitionSpec
	if err := json.Unmarshal([]byte(`{
		"spec-id": 1,
		"fields": [{"source-ids": [1, 2], "field-id": 1000, "name": "multi", "transform": "identity"}]
	}`), &spec); err != nil {
		t.Fatalf("parse partition spec: %v", err)
	}

	var model icebergTablePartitionSpec
	if err := model.FromIceberg(spec); err != nil {
		t.Fatalf("convert partition spec: %v", err)
	}
	if !slices.Equal(model.Fields[0].SourceIDs, []int64{1, 2}) {
		t.Fatalf("source IDs changed from [1 2] to %v", model.Fields[0].SourceIDs)
	}
}

func TestReproPreserveMultiSourceSortField(t *testing.T) {
	var order table.SortOrder
	if err := json.Unmarshal([]byte(`{
		"order-id": 1,
		"fields": [{"source-ids": [1, 2], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]
	}`), &order); err != nil {
		t.Fatalf("parse sort order: %v", err)
	}

	var model icebergTableSortOrder
	if err := model.FromIceberg(order); err != nil {
		t.Fatalf("convert sort order to Terraform model: %v", err)
	}
	roundTripped, err := model.ToIceberg()
	if err != nil {
		t.Fatalf("convert Terraform model to sort order: %v", err)
	}

	for _, field := range roundTripped.Fields() {
		if !slices.Equal(field.SourceIDs, []int{1, 2}) {
			t.Fatalf("source IDs changed from [1 2] to %v", field.SourceIDs)
		}
	}
}

func TestReproPreserveFormatV3FieldDefaults(t *testing.T) {
	original := iceberg.NewSchema(7,
		iceberg.NestedField{
			ID:             1,
			Name:           "status",
			Type:           iceberg.PrimitiveTypes.String,
			InitialDefault: "created",
			WriteDefault:   "pending",
		},
	)

	var model icebergTableSchema
	if err := model.FromIceberg(original); err != nil {
		t.Fatalf("convert schema to Terraform model: %v", err)
	}
	roundTripped, err := model.ToIceberg()
	if err != nil {
		t.Fatalf("convert Terraform model to schema: %v", err)
	}
	field := roundTripped.Fields()[0]
	if field.InitialDefault != "created" || field.WriteDefault != "pending" {
		t.Fatalf("field defaults changed from [created pending] to [%v %v]", field.InitialDefault, field.WriteDefault)
	}
}

func TestReproAllowDateToTimestampPromotionInV3(t *testing.T) {
	current := schemaOf(iceberg.PrimitiveTypes.Date, false)
	planned := schemaOf(iceberg.PrimitiveTypes.Timestamp, false)
	tbl := reproTable(t, current, 3)
	plan, state := reproSchemaPlanAndState(t, planned, current)
	var updateDiags diag.Diagnostics
	updates := (&icebergTableResource{}).calculateSchemaUpdates(context.Background(), &plan, &state, tbl, &updateDiags)

	if updateDiags.HasError() {
		t.Fatalf("expected v3 date-to-timestamp promotion to be accepted: %v", updateDiags.Errors())
	}
	if len(updates) != 2 {
		t.Fatalf("expected v3 date-to-timestamp promotion to produce 2 schema updates, got %d", len(updates))
	}
}

func TestReproReadUnknownPartitionTransform(t *testing.T) {
	var spec iceberg.PartitionSpec
	if err := json.Unmarshal([]byte(`{
		"spec-id": 1,
		"fields": [{"source-id": 1, "field-id": 1000, "name": "custom", "transform": "vendor-transform"}]
	}`), &spec); err != nil {
		t.Fatalf("expected an unknown transform to be readable: %v", err)
	}
}

func TestReproReadFormatV3GeospatialTypes(t *testing.T) {
	for _, typeName := range []string{"geometry", "geography"} {
		t.Run(typeName, func(t *testing.T) {
			var schema iceberg.Schema
			raw := `{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"location","required":false,"type":"` + typeName + `"}]}`
			if err := json.Unmarshal([]byte(raw), &schema); err != nil {
				t.Fatalf("expected format-v3 type %q to be readable: %v", typeName, err)
			}
		})
	}
}

func TestAccReproPreserveExplicitSchemaAndFieldIDs(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_CATALOG_URI")
	if catalogURI == "" {
		catalogURI = "http://localhost:8181"
	}

	config := fmt.Sprintf(providerConfig, catalogURI) + `
resource "iceberg_namespace" "explicit_ids" {
  name = ["repro_explicit_ids"]
}

resource "iceberg_table" "explicit_ids" {
  namespace = iceberg_namespace.explicit_ids.name
  name      = "explicit_ids"
  schema = {
    id = 7
    fields = [
      {
        id       = 10
        name     = "first"
        type     = "string"
        required = false
      },
      {
        id       = 20
        name     = "second"
        type     = "string"
        required = false
      }
    ]
  }
}
`

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_table.explicit_ids", "schema.id", "7"),
					resource.TestCheckResourceAttr("iceberg_table.explicit_ids", "schema.fields.0.id", "10"),
					resource.TestCheckResourceAttr("iceberg_table.explicit_ids", "schema.fields.1.id", "20"),
				),
			},
		},
	})
}
