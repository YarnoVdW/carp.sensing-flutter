// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'trigger_example.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

RemoteTrigger _$RemoteTriggerFromJson(Map<String, dynamic> json) =>
    RemoteTrigger(
      uri: json['uri'] as String,
      interval: json['interval'] == null
          ? const Duration(minutes: 10)
          : Duration(microseconds: (json['interval'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$RemoteTriggerToJson(RemoteTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'uri': instance.uri,
      'interval': instance.interval.inMicroseconds,
    };
