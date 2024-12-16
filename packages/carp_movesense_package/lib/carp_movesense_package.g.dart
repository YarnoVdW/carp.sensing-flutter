// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'carp_movesense_package.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

MovesenseDeviceInformation _$MovesenseDeviceInformationFromJson(
        Map<String, dynamic> json) =>
    MovesenseDeviceInformation(
      json['manufacturerName'] as String?,
      json['brandName'] as String?,
      json['productName'] as String?,
      json['variant'] as String?,
      json['design'] as String?,
      json['hardwareCompatibilityId'] as String?,
      json['serial'] as String?,
      json['pcbaSerial'] as String?,
      json['softwareVersion'] as String?,
      json['hardwareType'] as String?,
      json['additionalVersionInfo'] as String?,
      json['apiLevel'] as String?,
      json['address'] as String?,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MovesenseDeviceInformationToJson(
        MovesenseDeviceInformation instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sensorSpecificData case final value?)
        'sensorSpecificData': value,
      if (instance.manufacturerName case final value?)
        'manufacturerName': value,
      if (instance.brandName case final value?) 'brandName': value,
      if (instance.productName case final value?) 'productName': value,
      if (instance.variant case final value?) 'variant': value,
      if (instance.design case final value?) 'design': value,
      if (instance.hardwareCompatibilityId case final value?)
        'hardwareCompatibilityId': value,
      if (instance.serial case final value?) 'serial': value,
      if (instance.pcbaSerial case final value?) 'pcbaSerial': value,
      if (instance.softwareVersion case final value?) 'softwareVersion': value,
      if (instance.hardwareType case final value?) 'hardwareType': value,
      if (instance.additionalVersionInfo case final value?)
        'additionalVersionInfo': value,
      if (instance.apiLevel case final value?) 'apiLevel': value,
      if (instance.address case final value?) 'address': value,
    };

MovesenseStateChange _$MovesenseStateChangeFromJson(
        Map<String, dynamic> json) =>
    MovesenseStateChange(
      $enumDecode(_$MovesenseDeviceStateEnumMap, json['state']),
      (json['timestamp'] as num?)?.toInt(),
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MovesenseStateChangeToJson(
        MovesenseStateChange instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sensorSpecificData case final value?)
        'sensorSpecificData': value,
      'state': _$MovesenseDeviceStateEnumMap[instance.state]!,
      'timestamp': instance.timestamp,
    };

const _$MovesenseDeviceStateEnumMap = {
  MovesenseDeviceState.unknown: 'unknown',
  MovesenseDeviceState.moving: 'moving',
  MovesenseDeviceState.notMoving: 'notMoving',
  MovesenseDeviceState.connected: 'connected',
  MovesenseDeviceState.disconnected: 'disconnected',
  MovesenseDeviceState.tap: 'tap',
  MovesenseDeviceState.doubleTap: 'doubleTap',
  MovesenseDeviceState.acceleration: 'acceleration',
  MovesenseDeviceState.freeFall: 'freeFall',
};

MovesenseHR _$MovesenseHRFromJson(Map<String, dynamic> json) => MovesenseHR(
      (json['hr'] as num).toDouble(),
      (json['rr'] as num?)?.toInt(),
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MovesenseHRToJson(MovesenseHR instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sensorSpecificData case final value?)
        'sensorSpecificData': value,
      'hr': instance.hr,
      if (instance.rr case final value?) 'rr': value,
    };

MovesenseECG _$MovesenseECGFromJson(Map<String, dynamic> json) => MovesenseECG(
      (json['timestamp'] as num).toInt(),
      (json['samples'] as List<dynamic>)
          .map((e) => (e as num).toInt())
          .toList(),
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MovesenseECGToJson(MovesenseECG instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sensorSpecificData case final value?)
        'sensorSpecificData': value,
      'timestamp': instance.timestamp,
      'samples': instance.samples,
    };

MovesenseTemperature _$MovesenseTemperatureFromJson(
        Map<String, dynamic> json) =>
    MovesenseTemperature(
      (json['timestamp'] as num).toInt(),
      (json['measurement'] as num).toInt(),
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MovesenseTemperatureToJson(
        MovesenseTemperature instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sensorSpecificData case final value?)
        'sensorSpecificData': value,
      'timestamp': instance.timestamp,
      'measurement': instance.measurement,
    };

MovesenseIMU _$MovesenseIMUFromJson(Map<String, dynamic> json) => MovesenseIMU(
      (json['timestamp'] as num).toInt(),
      (json['accelerometer'] as List<dynamic>)
          .map((e) =>
              MovesenseAccelerometerSample.fromJson(e as Map<String, dynamic>))
          .toList(),
      (json['gyroscope'] as List<dynamic>)
          .map((e) =>
              MovesenseGyroscopeSample.fromJson(e as Map<String, dynamic>))
          .toList(),
      (json['magnetometer'] as List<dynamic>)
          .map((e) =>
              MovesenseMagnetometerSample.fromJson(e as Map<String, dynamic>))
          .toList(),
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MovesenseIMUToJson(MovesenseIMU instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sensorSpecificData case final value?)
        'sensorSpecificData': value,
      'timestamp': instance.timestamp,
      'accelerometer': instance.accelerometer,
      'gyroscope': instance.gyroscope,
      'magnetometer': instance.magnetometer,
    };

MovesenseAccelerometerSample _$MovesenseAccelerometerSampleFromJson(
        Map<String, dynamic> json) =>
    MovesenseAccelerometerSample(
      json['x'] as num,
      json['y'] as num,
      json['z'] as num,
    );

Map<String, dynamic> _$MovesenseAccelerometerSampleToJson(
        MovesenseAccelerometerSample instance) =>
    <String, dynamic>{
      'x': instance.x,
      'y': instance.y,
      'z': instance.z,
    };

MovesenseGyroscopeSample _$MovesenseGyroscopeSampleFromJson(
        Map<String, dynamic> json) =>
    MovesenseGyroscopeSample(
      json['x'] as num,
      json['y'] as num,
      json['z'] as num,
    );

Map<String, dynamic> _$MovesenseGyroscopeSampleToJson(
        MovesenseGyroscopeSample instance) =>
    <String, dynamic>{
      'x': instance.x,
      'y': instance.y,
      'z': instance.z,
    };

MovesenseMagnetometerSample _$MovesenseMagnetometerSampleFromJson(
        Map<String, dynamic> json) =>
    MovesenseMagnetometerSample(
      json['x'] as num,
      json['y'] as num,
      json['z'] as num,
    );

Map<String, dynamic> _$MovesenseMagnetometerSampleToJson(
        MovesenseMagnetometerSample instance) =>
    <String, dynamic>{
      'x': instance.x,
      'y': instance.y,
      'z': instance.z,
    };

MovesenseDevice _$MovesenseDeviceFromJson(Map<String, dynamic> json) =>
    MovesenseDevice(
      roleName:
          json['roleName'] as String? ?? MovesenseDevice.DEFAULT_ROLE_NAME,
      isOptional: json['isOptional'] as bool? ?? true,
      name: json['name'] as String?,
      address: json['address'] as String?,
      serial: json['serial'] as String?,
      deviceType: $enumDecodeNullable(
              _$MovesenseDeviceTypeEnumMap, json['deviceType']) ??
          MovesenseDeviceType.UNKNOWN,
    )
      ..$type = json['__type'] as String?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      );

Map<String, dynamic> _$MovesenseDeviceToJson(MovesenseDevice instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'roleName': instance.roleName,
      if (instance.isOptional case final value?) 'isOptional': value,
      if (instance.defaultSamplingConfiguration case final value?)
        'defaultSamplingConfiguration': value,
      if (instance.address case final value?) 'address': value,
      if (instance.serial case final value?) 'serial': value,
      if (instance.name case final value?) 'name': value,
      'deviceType': _$MovesenseDeviceTypeEnumMap[instance.deviceType]!,
    };

const _$MovesenseDeviceTypeEnumMap = {
  MovesenseDeviceType.UNKNOWN: 'UNKNOWN',
  MovesenseDeviceType.MD: 'MD',
  MovesenseDeviceType.HR_PLUS: 'HR_PLUS',
  MovesenseDeviceType.HR2: 'HR2',
  MovesenseDeviceType.FLASH: 'FLASH',
};
