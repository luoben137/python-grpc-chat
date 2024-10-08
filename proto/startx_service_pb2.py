# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/startx_service.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='proto/startx_service.proto',
  package='grpc',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1aproto/startx_service.proto\x12\x04grpc\"\x07\n\x05\x45mpty\"%\n\x04Note\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0f\n\x07message\x18\x02 \x01(\t\"\x14\n\x04User\x12\x0c\n\x04name\x18\x01 \x01(\t2\x83\x01\n\x0cStartxServer\x12)\n\rMessageStream\x12\n.grpc.User\x1a\n.grpc.Note0\x01\x12#\n\x08SendNote\x12\n.grpc.Note\x1a\x0b.grpc.Empty\x12#\n\x08Register\x12\n.grpc.User\x1a\x0b.grpc.Emptyb\x06proto3'
)




_EMPTY = _descriptor.Descriptor(
  name='Empty',
  full_name='grpc.Empty',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=36,
  serialized_end=43,
)


_NOTE = _descriptor.Descriptor(
  name='Note',
  full_name='grpc.Note',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='grpc.Note.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='message', full_name='grpc.Note.message', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=45,
  serialized_end=82,
)


_USER = _descriptor.Descriptor(
  name='User',
  full_name='grpc.User',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='grpc.User.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=84,
  serialized_end=104,
)

DESCRIPTOR.message_types_by_name['Empty'] = _EMPTY
DESCRIPTOR.message_types_by_name['Note'] = _NOTE
DESCRIPTOR.message_types_by_name['User'] = _USER
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Empty = _reflection.GeneratedProtocolMessageType('Empty', (_message.Message,), {
  'DESCRIPTOR' : _EMPTY,
  '__module__' : 'proto.startx_service_pb2'
  # @@protoc_insertion_point(class_scope:grpc.Empty)
  })
_sym_db.RegisterMessage(Empty)

Note = _reflection.GeneratedProtocolMessageType('Note', (_message.Message,), {
  'DESCRIPTOR' : _NOTE,
  '__module__' : 'proto.startx_service_pb2'
  # @@protoc_insertion_point(class_scope:grpc.Note)
  })
_sym_db.RegisterMessage(Note)

User = _reflection.GeneratedProtocolMessageType('User', (_message.Message,), {
  'DESCRIPTOR' : _USER,
  '__module__' : 'proto.startx_service_pb2'
  # @@protoc_insertion_point(class_scope:grpc.User)
  })
_sym_db.RegisterMessage(User)



_STARTXSERVER = _descriptor.ServiceDescriptor(
  name='StartxServer',
  full_name='grpc.StartxServer',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=107,
  serialized_end=238,
  methods=[
  _descriptor.MethodDescriptor(
    name='MessageStream',
    full_name='grpc.StartxServer.MessageStream',
    index=0,
    containing_service=None,
    input_type=_USER,
    output_type=_NOTE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='SendNote',
    full_name='grpc.StartxServer.SendNote',
    index=1,
    containing_service=None,
    input_type=_NOTE,
    output_type=_EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='Register',
    full_name='grpc.StartxServer.Register',
    index=2,
    containing_service=None,
    input_type=_USER,
    output_type=_EMPTY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_STARTXSERVER)

DESCRIPTOR.services_by_name['StartxServer'] = _STARTXSERVER

# @@protoc_insertion_point(module_scope)
