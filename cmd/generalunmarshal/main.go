package main

import (
	"fmt"
	"os"
	"time"

	dp_contextv1 "github.com/Unity-Technologies/ugs-data-terraform-workspace/gen/go/dp_context/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var sysUsageJson string = `{
"ts":1653939937216,
"eventId":"e1defb82-f74d-4f47-b059-f97320b700cf",
"fingerprint":"e1defb82-f74d-4f47-b059-f97320b700cf",
"serviceId":"MP",
"projectId":"cee481bf-727c-4afb-b99c-ae2c9f87f373",
"environmentId":"prd",
"playerId":"",
"startTime":1653939600000,
"endTime":1653939900000,
"type":"network_usage_event",
"amount":0.000044216401875019073,
"tags":{
"multiplayFleetId":"5b2546cc-a55c-11eb-b85e-0242ac110002",
"multiplayMachineId":"2575272",
"multiplayInfraType":"Physical",
"multiplayProjectId":"ASID:448980",
"multiplayRegion":"f7e2fdd1-f05b-4cc8-ad48-eb46e7e6f3f5",
"analyticsEventType":"",
"analyticsEventName":""
}
}`

var invUsageJson string = `{
"ts":1653422520000,
"eventId":"dae84cfa-2304-45c5-adc7-6ddfdf3aba75",
"fingerprint":"1977cb50cf4d31063e96fb4735875a2c",
"serviceId":"cloud-save",
"projectId":"497c4241-c043-4ffd-bbe4-6515168c3c92",
"environmentId":"1decdbfc-6a3e-4b69-bca5-0c16f3f13911",
"resourceType":"None",
"resource":"None",
"source":"None",
"correlationId":"1f766fa8-9937-416d-877a-16f241800811",
"inputSize":0,
"outputSize":1,
"duration":0,
"tags":{
"httpMethod":"GET",
"httpStatus":200
}
}`
var invUsageJsonBytes []byte = []byte(invUsageJson)
var sysUsageJsonBytes []byte = []byte(sysUsageJson)

var events map[string][]byte = make(map[string][]byte)
var _ protodesc.Resolver = (*protoregistry.Files)(nil)

// generic handling of proto unmarshaling and marshaling
func main() {
	// registry from descriptor set
	b, err := os.ReadFile("foo.bin")
	if err != nil {
		panic(err)
	}
	fds := &descriptorpb.FileDescriptorSet{}
	err = proto.Unmarshal(b, fds)
	if err != nil {
		panic(err)
	}
	// Fix schema messages based on
	files := fds.GetFile()
	for _, file := range files {
		fmt.Println(*file.Name)
		for _, msgtype := range file.MessageType {
			if msgtype.GetName() == "SystemUsage" {
				newmsg := proto.Clone(msgtype.ProtoReflect().Interface()).(*descriptorpb.DescriptorProto)
				newmsg.Name = proto.String("SystemUsage_fixed")
				for _, f := range newmsg.Field {
					if *f.Name == "end_time" {
						newtype := descriptorpb.FieldDescriptorProto_TYPE_FLOAT
						f.Type = &newtype
					}
				}
				file.MessageType = append(file.MessageType, newmsg)
			}
		}
	}
	ff, err := protodesc.NewFiles(fds)
	if err != nil {
		panic(err)
	}

	events["inv"] = invUsageJsonBytes
	events["sys"] = sysUsageJsonBytes
	for type_, event := range events {
		var serialized []byte
		switch type_ {
		case "inv":
			mdesc, err := ff.FindDescriptorByName("invocation_usage.v3.InvocationUsage")
			if err != nil {
				panic(err)
			}
			msgdesc, ok := mdesc.(protoreflect.MessageDescriptor)
			msgdesc.ParentFile()
			if !ok {
				panic(fmt.Errorf("type assertion to MessageDescriptor failed: %s", mdesc.FullName()))
			}

			msg := dynamicpb.NewMessage(msgdesc)
			serialized = serialize(msg, event)
		case "sys":
			mdesc, err := ff.FindDescriptorByName("system_usage.v3.SystemUsage")
			if err != nil {
				panic(err)
			}
			msgdesc, ok := mdesc.(protoreflect.MessageDescriptor)
			if !ok {
				panic(fmt.Errorf("type assertion to MessageDescriptor failed: %s", mdesc.FullName()))
			}
			msg := dynamicpb.NewMessage(msgdesc)
			serialized = serialize(msg, event)
		}
		serialized = serialized
	}
}

func serialize(msg protoreflect.ProtoMessage, event []byte) []byte {
	// Unmarshal/Validate event schema.
	err := protojson.Unmarshal(event, msg)
	if err != nil {
		panic(err)
	}

	// Set fields controlled by pipeline
	context := &dp_contextv1.Context{
		KafkaContext: &dp_contextv1.Context_KafkaContext{
			Offset:    proto.Int64(1),
			Partition: proto.Int64(2),
		},
	}
	msg.ProtoReflect().Set(msg.ProtoReflect().Descriptor().Fields().ByName("submit_time"), protoreflect.ValueOfInt64(time.Now().UnixMicro()))
	msg.ProtoReflect().Set(msg.ProtoReflect().Descriptor().Fields().ByName("dp_context"), protoreflect.ValueOfMessage(context.ProtoReflect()))

	// serialize
	reflectedserialized, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return reflectedserialized
}
