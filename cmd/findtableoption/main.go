package main

import (
	"fmt"
	"os"

	"github.com/Unity-Technologies/ugs-data-terraform-workspace/gen/go/gen_bq_schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

var sysUsageJson string = `{
"submit_time":1655109158000,
"super_secret":"jacob_moller@unity3d.com"
}`

var sysUsageJsonBytes []byte = []byte(sysUsageJson)

var events map[string][]byte = make(map[string][]byte)
var _ protodesc.Resolver = (*protoregistry.Files)(nil)

// generic handling of proto unmarshaling and marshaling
func main() {
	// registry from descriptor set
	b, err := os.ReadFile("descriptor.bin")
	if err != nil {
		panic(err)
	}
	fds := &descriptorpb.FileDescriptorSet{}
	err = proto.Unmarshal(b, fds)
	if err != nil {
		panic(err)
	}

	ff, err := protodesc.NewFiles(fds)
	if err != nil {
		panic(err)
	}

	ff.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Messages().Len(); i++ {
			msgdesc := fd.Messages().Get(i)
			optspb, ok := msgdesc.Options().(*descriptorpb.MessageOptions)
			if !ok || optspb == nil {
				return true
			}
			msgOpts := proto.GetExtension(optspb, gen_bq_schema.E_BigqueryOpts)
			msgoption, ok := msgOpts.(*gen_bq_schema.BigQueryMessageOptions)
			if !ok {
				return true
			}
			fmt.Println(msgoption.GetTableName())
		}
		return true
	})
}
