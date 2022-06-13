package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var sysUsageJson string = `{
"submit_time":1655112266055539,
"super_secret":"jacob_moller@unity3d.com",
"tags": {
	"tag_field": "foofofof"
}
}`

var sysUsageJson1 string = `{
"submit_time":1655112266055539,
"super_secret": "jacob_moller@unity3d.com"
}`

var sysUsageJsonBytes []byte = []byte(sysUsageJson)
var sysUsageJsonBytes1 []byte = []byte(sysUsageJson1)

var events [][]byte = make([][]byte, 0)
var _ protodesc.Resolver = (*protoregistry.Files)(nil)

func BigQuerySchemaToProtobufDescriptor(schema bigquery.Schema) (*descriptorpb.DescriptorProto, protoreflect.MessageDescriptor, error) {
	convertedSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("adapt.BQSchemaToStorageTableSchema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(convertedSchema, "jacob_test_v1")
	if err != nil {
		return nil, nil, fmt.Errorf("adapt.StorageSchemaToDescriptor: %w", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		// nolint: goerr113
		return nil, nil, errors.New("adapted descriptor is not a message descriptor")
	}
	protobufDescriptor, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, fmt.Errorf("adapt.NormalizeDescriptor: %w", err)
	}
	return protobufDescriptor, messageDescriptor, nil
}

// generic handling of proto unmarshaling and marshaling
func main() {
	ctx := context.Background()
	bqclient, err := bigquery.NewClient(ctx, "unity-ai-data-ugs-test")
	if err != nil {
		panic(err)
	}

	mngdclient, err := managedwriter.NewClient(ctx, "unity-ai-data-ugs-test")
	if err != nil {
		panic(err)
	}
	dataset := bqclient.Dataset("unity_services")
	md, err := dataset.Table("jacob_test_v1").Metadata(ctx)
	if err != nil {
		panic(err)
	}
	normalized, messageDescriptor, err := BigQuerySchemaToProtobufDescriptor(md.Schema)
	if err != nil {
		panic(err)
	}

	tableName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", "unity-ai-data-ugs-test", "unity_services", "jacob_test_v1")
	managedStream, err := mngdclient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(tableName),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(normalized))
	if err != nil {
		panic(err)
	}

	// // registry from descriptor set
	// b, err := os.ReadFile("descriptor.bin")
	// if err != nil {
	// 	panic(err)
	// }
	// fds := &descriptorpb.FileDescriptorSet{}
	// err = proto.Unmarshal(b, fds)
	// if err != nil {
	// 	panic(err)
	// }

	// ff, err := protodesc.NewFiles(fds)
	// if err != nil {
	// 	panic(err)
	// }

	events = append(events, sysUsageJsonBytes)
	events = append(events, sysUsageJsonBytes1)

	encoded := make([][]byte, 0)
	for _, event := range events {

		// mdesc, err := ff.FindDescriptorByName("jacob_test.v1.JacobTest")
		// if err != nil {
		// 	panic(err)
		// }

		// msgdesc, ok := mdesc.(protoreflect.MessageDescriptor)
		// if !ok {
		// 	panic(fmt.Errorf("type assertion to MessageDescriptor failed: %s", mdesc.FullName()))
		// }

		msg := dynamicpb.NewMessage(messageDescriptor)
		serialized := serialize(msg, event)
		encoded = append(encoded, serialized)
	}
	result, err := managedStream.AppendRows(ctx, encoded, managedwriter.WithOffset(0))
	if err != nil {
		panic(err)
	}
	// Block until the write is complete and return the result.
	start := time.Now()
	returnedOffset, err := result.GetResult(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(time.Since(start), returnedOffset)
}

func serialize(msg protoreflect.ProtoMessage, event []byte) []byte {
	// Unmarshal/Validate event schema.
	err := protojson.Unmarshal(event, msg)
	if err != nil {
		panic(err)
	}
	// serialize
	reflectedserialized, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return reflectedserialized
}
