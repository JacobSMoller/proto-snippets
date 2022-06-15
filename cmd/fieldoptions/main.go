package main

import (
	"fmt"

	gen_bq_schema "github.com/GoogleCloudPlatform/protoc-gen-bq-schema/protos"
	"google.golang.org/protobuf/proto"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
)

// Using proto field options
func main() {
	msg := SystemUsage{}
	reflected := msg.ProtoReflect()
	field := reflected.Descriptor().Fields().ByName("foo")
	opts := field.Options().(*descriptorpb.FieldOptions)
	fmt.Println(opts)
	s := proto.GetExtension(opts, gen_bq_schema.E_Bigquery)
	option, ok := s.(*gen_bq_schema.BigQueryFieldOptions)
	if !ok {
		panic("opsdifjosdijf")
	}
	fmt.Println(option)
}
