package main

import (
	"fmt"

	gen_bq_schema "github.com/Unity-Technologies/ugs-data-terraform-workspace/gen/go/gen_bq_schema"
	su "github.com/Unity-Technologies/ugs-data-terraform-workspace/gen/go/system_usage/v3"
	"google.golang.org/protobuf/proto"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

// Using proto field options
func main() {
	msg := su.SystemUsage{}
	reflected := msg.ProtoReflect()
	field := reflected.Descriptor().Fields().ByName("ts")
	opts := field.Options().(*descriptorpb.FieldOptions)
	s := proto.GetExtension(opts, gen_bq_schema.E_Bigquery)
	option, ok := s.(*gen_bq_schema.BigQueryFieldOptions)
	if !ok {
		panic(ok)
	}
	fmt.Println(option.Require, option.TypeOverride)
}
