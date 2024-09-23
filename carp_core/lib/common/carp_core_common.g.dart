// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'carp_core_common.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

Account _$AccountFromJson(Map<String, dynamic> json) => Account(
      id: json['id'] as String?,
      identity:
          AccountIdentity.fromJson(json['identity'] as Map<String, dynamic>),
    );

Map<String, dynamic> _$AccountToJson(Account instance) => <String, dynamic>{
      'identity': instance.identity.toJson(),
      'id': instance.id,
    };

AccountIdentity _$AccountIdentityFromJson(Map<String, dynamic> json) =>
    AccountIdentity()..$type = json['__type'] as String?;

Map<String, dynamic> _$AccountIdentityToJson(AccountIdentity instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  return val;
}

EmailAccountIdentity _$EmailAccountIdentityFromJson(
        Map<String, dynamic> json) =>
    EmailAccountIdentity(
      json['emailAddress'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$EmailAccountIdentityToJson(
    EmailAccountIdentity instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['emailAddress'] = instance.emailAddress;
  return val;
}

UsernameAccountIdentity _$UsernameAccountIdentityFromJson(
        Map<String, dynamic> json) =>
    UsernameAccountIdentity(
      json['username'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$UsernameAccountIdentityToJson(
    UsernameAccountIdentity instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['username'] = instance.username;
  return val;
}

ParticipantRole _$ParticipantRoleFromJson(Map<String, dynamic> json) =>
    ParticipantRole(
      json['role'] as String,
      json['isOptional'] as bool? ?? false,
    );

Map<String, dynamic> _$ParticipantRoleToJson(ParticipantRole instance) =>
    <String, dynamic>{
      'role': instance.role,
      'isOptional': instance.isOptional,
    };

ExpectedParticipantData _$ExpectedParticipantDataFromJson(
        Map<String, dynamic> json) =>
    ExpectedParticipantData(
      attribute: json['attribute'] == null
          ? null
          : ParticipantAttribute.fromJson(
              json['attribute'] as Map<String, dynamic>),
      assignedTo: json['assignedTo'] == null
          ? null
          : AssignedTo.fromJson(json['assignedTo'] as Map<String, dynamic>),
    );

Map<String, dynamic> _$ExpectedParticipantDataToJson(
    ExpectedParticipantData instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('attribute', instance.attribute?.toJson());
  val['assignedTo'] = instance.assignedTo.toJson();
  return val;
}

ParticipantAttribute _$ParticipantAttributeFromJson(
        Map<String, dynamic> json) =>
    ParticipantAttribute(
      inputDataType: json['inputDataType'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$ParticipantAttributeToJson(
    ParticipantAttribute instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['inputDataType'] = instance.inputDataType;
  return val;
}

AssignedTo _$AssignedToFromJson(Map<String, dynamic> json) => AssignedTo(
      roleNames: (json['roleNames'] as List<dynamic>?)
          ?.map((e) => e as String)
          .toSet(),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$AssignedToToJson(AssignedTo instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('roleNames', instance.roleNames?.toList());
  return val;
}

Measure _$MeasureFromJson(Map<String, dynamic> json) => Measure(
      type: json['type'] as String,
    )
      ..$type = json['__type'] as String?
      ..overrideSamplingConfiguration = json['overrideSamplingConfiguration'] ==
              null
          ? null
          : SamplingConfiguration.fromJson(
              json['overrideSamplingConfiguration'] as Map<String, dynamic>);

Map<String, dynamic> _$MeasureToJson(Measure instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['type'] = instance.type;
  writeNotNull('overrideSamplingConfiguration',
      instance.overrideSamplingConfiguration?.toJson());
  return val;
}

TaskConfiguration _$TaskConfigurationFromJson(Map<String, dynamic> json) =>
    TaskConfiguration(
      name: json['name'] as String?,
      description: json['description'] as String?,
      measures: (json['measures'] as List<dynamic>?)
          ?.map((e) => Measure.fromJson(e as Map<String, dynamic>))
          .toList(),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$TaskConfigurationToJson(TaskConfiguration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['name'] = instance.name;
  writeNotNull('measures', instance.measures?.map((e) => e.toJson()).toList());
  writeNotNull('description', instance.description);
  return val;
}

BackgroundTask _$BackgroundTaskFromJson(Map<String, dynamic> json) =>
    BackgroundTask(
      name: json['name'] as String?,
      description: json['description'] as String?,
      measures: (json['measures'] as List<dynamic>?)
          ?.map((e) => Measure.fromJson(e as Map<String, dynamic>))
          .toList(),
      duration: _$IsoDurationFromJson(json['duration'] as String?),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$BackgroundTaskToJson(BackgroundTask instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['name'] = instance.name;
  writeNotNull('measures', instance.measures?.map((e) => e.toJson()).toList());
  writeNotNull('description', instance.description);
  writeNotNull('duration', _$IsoDurationToJson(instance.duration));
  return val;
}

CustomProtocolTask _$CustomProtocolTaskFromJson(Map<String, dynamic> json) =>
    CustomProtocolTask(
      name: json['name'] as String?,
      description: json['description'] as String?,
      studyProtocol: json['studyProtocol'] as String,
    )
      ..$type = json['__type'] as String?
      ..measures = (json['measures'] as List<dynamic>?)
          ?.map((e) => Measure.fromJson(e as Map<String, dynamic>))
          .toList();

Map<String, dynamic> _$CustomProtocolTaskToJson(CustomProtocolTask instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['name'] = instance.name;
  writeNotNull('measures', instance.measures?.map((e) => e.toJson()).toList());
  writeNotNull('description', instance.description);
  val['studyProtocol'] = instance.studyProtocol;
  return val;
}

WebTask _$WebTaskFromJson(Map<String, dynamic> json) => WebTask(
      name: json['name'] as String?,
      description: json['description'] as String?,
      measures: (json['measures'] as List<dynamic>?)
          ?.map((e) => Measure.fromJson(e as Map<String, dynamic>))
          .toList(),
      url: json['url'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$WebTaskToJson(WebTask instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['name'] = instance.name;
  writeNotNull('measures', instance.measures?.map((e) => e.toJson()).toList());
  writeNotNull('description', instance.description);
  val['url'] = instance.url;
  return val;
}

TaskControl _$TaskControlFromJson(Map<String, dynamic> json) => TaskControl(
      triggerId: (json['triggerId'] as num).toInt(),
      control: $enumDecodeNullable(_$ControlEnumMap, json['control']) ??
          Control.Start,
    )
      ..taskName = json['taskName'] as String
      ..destinationDeviceRoleName = json['destinationDeviceRoleName'] as String?
      ..hasBeenScheduledUntil = json['hasBeenScheduledUntil'] == null
          ? null
          : DateTime.parse(json['hasBeenScheduledUntil'] as String);

Map<String, dynamic> _$TaskControlToJson(TaskControl instance) {
  final val = <String, dynamic>{
    'triggerId': instance.triggerId,
    'taskName': instance.taskName,
  };

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('destinationDeviceRoleName', instance.destinationDeviceRoleName);
  val['control'] = _$ControlEnumMap[instance.control]!;
  writeNotNull('hasBeenScheduledUntil',
      instance.hasBeenScheduledUntil?.toIso8601String());
  return val;
}

const _$ControlEnumMap = {
  Control.Start: 'Start',
  Control.Stop: 'Stop',
};

DeviceConfiguration<TRegistration> _$DeviceConfigurationFromJson<
        TRegistration extends DeviceRegistration>(Map<String, dynamic> json) =>
    DeviceConfiguration<TRegistration>(
      roleName: json['roleName'] as String,
      isOptional: json['isOptional'] as bool?,
    )
      ..$type = json['__type'] as String?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      );

Map<String, dynamic>
    _$DeviceConfigurationToJson<TRegistration extends DeviceRegistration>(
        DeviceConfiguration<TRegistration> instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  return val;
}

DefaultDeviceConfiguration _$DefaultDeviceConfigurationFromJson(
        Map<String, dynamic> json) =>
    DefaultDeviceConfiguration(
      roleName: json['roleName'] as String,
      isOptional: json['isOptional'] as bool?,
    )
      ..$type = json['__type'] as String?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      );

Map<String, dynamic> _$DefaultDeviceConfigurationToJson(
    DefaultDeviceConfiguration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  return val;
}

PrimaryDeviceConfiguration<TRegistration> _$PrimaryDeviceConfigurationFromJson<
        TRegistration extends DeviceRegistration>(Map<String, dynamic> json) =>
    PrimaryDeviceConfiguration<TRegistration>(
      roleName: json['roleName'] as String,
    )
      ..$type = json['__type'] as String?
      ..isOptional = json['isOptional'] as bool?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      )
      ..isPrimaryDevice = json['isPrimaryDevice'] as bool;

Map<String, dynamic> _$PrimaryDeviceConfigurationToJson<
        TRegistration extends DeviceRegistration>(
    PrimaryDeviceConfiguration<TRegistration> instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  val['isPrimaryDevice'] = instance.isPrimaryDevice;
  return val;
}

CustomProtocolDevice _$CustomProtocolDeviceFromJson(
        Map<String, dynamic> json) =>
    CustomProtocolDevice(
      roleName:
          json['roleName'] as String? ?? CustomProtocolDevice.DEFAULT_ROLE_NAME,
    )
      ..$type = json['__type'] as String?
      ..isOptional = json['isOptional'] as bool?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      )
      ..isPrimaryDevice = json['isPrimaryDevice'] as bool;

Map<String, dynamic> _$CustomProtocolDeviceToJson(
    CustomProtocolDevice instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  val['isPrimaryDevice'] = instance.isPrimaryDevice;
  return val;
}

DeviceRegistration _$DeviceRegistrationFromJson(Map<String, dynamic> json) =>
    DeviceRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$DeviceRegistrationToJson(DeviceRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
    };

DefaultDeviceRegistration _$DefaultDeviceRegistrationFromJson(
        Map<String, dynamic> json) =>
    DefaultDeviceRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$DefaultDeviceRegistrationToJson(
        DefaultDeviceRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
    };

MACAddressDeviceRegistration _$MACAddressDeviceRegistrationFromJson(
        Map<String, dynamic> json) =>
    MACAddressDeviceRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
      address: json['address'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$MACAddressDeviceRegistrationToJson(
        MACAddressDeviceRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
      'address': instance.address,
    };

Smartphone _$SmartphoneFromJson(Map<String, dynamic> json) => Smartphone(
      roleName: json['roleName'] as String? ?? Smartphone.DEFAULT_ROLE_NAME,
    )
      ..$type = json['__type'] as String?
      ..isOptional = json['isOptional'] as bool?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      )
      ..isPrimaryDevice = json['isPrimaryDevice'] as bool;

Map<String, dynamic> _$SmartphoneToJson(Smartphone instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  val['isPrimaryDevice'] = instance.isPrimaryDevice;
  return val;
}

SmartphoneDeviceRegistration _$SmartphoneDeviceRegistrationFromJson(
        Map<String, dynamic> json) =>
    SmartphoneDeviceRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
      platform: json['platform'] as String?,
      hardware: json['hardware'] as String?,
      deviceName: json['deviceName'] as String?,
      deviceManufacturer: json['deviceManufacturer'] as String?,
      deviceModel: json['deviceModel'] as String?,
      operatingSystem: json['operatingSystem'] as String?,
      sdk: json['sdk'] as String?,
      release: json['release'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$SmartphoneDeviceRegistrationToJson(
        SmartphoneDeviceRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
      'platform': instance.platform,
      'hardware': instance.hardware,
      'deviceName': instance.deviceName,
      'deviceManufacturer': instance.deviceManufacturer,
      'deviceModel': instance.deviceModel,
      'operatingSystem': instance.operatingSystem,
      'sdk': instance.sdk,
      'release': instance.release,
    };

PersonalComputer _$PersonalComputerFromJson(Map<String, dynamic> json) =>
    PersonalComputer(
      roleName:
          json['roleName'] as String? ?? PersonalComputer.DEFAULT_ROLE_NAME,
    )
      ..$type = json['__type'] as String?
      ..isOptional = json['isOptional'] as bool?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      )
      ..isPrimaryDevice = json['isPrimaryDevice'] as bool;

Map<String, dynamic> _$PersonalComputerToJson(PersonalComputer instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  val['isPrimaryDevice'] = instance.isPrimaryDevice;
  return val;
}

PersonalComputerRegistration _$PersonalComputerRegistrationFromJson(
        Map<String, dynamic> json) =>
    PersonalComputerRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
      platform: json['platform'] as String?,
      computerName: json['computerName'] as String?,
      memorySize: (json['memorySize'] as num?)?.toInt(),
      deviceModel: json['deviceModel'] as String?,
      operatingSystem: json['operatingSystem'] as String?,
      version: json['version'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$PersonalComputerRegistrationToJson(
        PersonalComputerRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
      'platform': instance.platform,
      'computerName': instance.computerName,
      'memorySize': instance.memorySize,
      'deviceModel': instance.deviceModel,
      'operatingSystem': instance.operatingSystem,
      'version': instance.version,
    };

WebBrowser _$WebBrowserFromJson(Map<String, dynamic> json) => WebBrowser(
      roleName: json['roleName'] as String? ?? WebBrowser.DEFAULT_ROLE_NAME,
    )
      ..$type = json['__type'] as String?
      ..isOptional = json['isOptional'] as bool?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      )
      ..isPrimaryDevice = json['isPrimaryDevice'] as bool;

Map<String, dynamic> _$WebBrowserToJson(WebBrowser instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  val['isPrimaryDevice'] = instance.isPrimaryDevice;
  return val;
}

WebBrowserRegistration _$WebBrowserRegistrationFromJson(
        Map<String, dynamic> json) =>
    WebBrowserRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
      browserName: json['browserName'] as String?,
      deviceMemory: (json['deviceMemory'] as num?)?.toInt(),
      language: json['language'] as String?,
      vendor: json['vendor'] as String?,
      maxTouchPoints: (json['maxTouchPoints'] as num?)?.toInt(),
      hardwareConcurrency: (json['hardwareConcurrency'] as num?)?.toInt(),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$WebBrowserRegistrationToJson(
        WebBrowserRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
      'browserName': instance.browserName,
      'deviceMemory': instance.deviceMemory,
      'language': instance.language,
      'vendor': instance.vendor,
      'maxTouchPoints': instance.maxTouchPoints,
      'hardwareConcurrency': instance.hardwareConcurrency,
    };

AltBeacon _$AltBeaconFromJson(Map<String, dynamic> json) => AltBeacon(
      roleName: json['roleName'] as String? ?? 'Beacon',
    )
      ..$type = json['__type'] as String?
      ..isOptional = json['isOptional'] as bool?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      );

Map<String, dynamic> _$AltBeaconToJson(AltBeacon instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  return val;
}

AltBeaconDeviceRegistration _$AltBeaconDeviceRegistrationFromJson(
        Map<String, dynamic> json) =>
    AltBeaconDeviceRegistration(
      deviceId: json['deviceId'] as String?,
      deviceDisplayName: json['deviceDisplayName'] as String?,
      registrationCreatedOn: json['registrationCreatedOn'] == null
          ? null
          : DateTime.parse(json['registrationCreatedOn'] as String),
      manufacturerId: (json['manufacturerId'] as num?)?.toInt(),
      organizationId: json['organizationId'] as String?,
      majorId: (json['majorId'] as num?)?.toInt(),
      minorId: (json['minorId'] as num?)?.toInt(),
      referenceRssi: (json['referenceRssi'] as num?)?.toInt(),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$AltBeaconDeviceRegistrationToJson(
        AltBeaconDeviceRegistration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'deviceId': instance.deviceId,
      'deviceDisplayName': instance.deviceDisplayName,
      'registrationCreatedOn': instance.registrationCreatedOn.toIso8601String(),
      'manufacturerId': instance.manufacturerId,
      'organizationId': instance.organizationId,
      'majorId': instance.majorId,
      'minorId': instance.minorId,
      'referenceRssi': instance.referenceRssi,
    };

BLEHeartRateDevice _$BLEHeartRateDeviceFromJson(Map<String, dynamic> json) =>
    BLEHeartRateDevice(
      roleName: json['roleName'] as String,
      isOptional: json['isOptional'] as bool? ?? true,
    )
      ..$type = json['__type'] as String?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      );

Map<String, dynamic> _$BLEHeartRateDeviceToJson(BLEHeartRateDevice instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['roleName'] = instance.roleName;
  writeNotNull('isOptional', instance.isOptional);
  writeNotNull(
      'defaultSamplingConfiguration',
      instance.defaultSamplingConfiguration
          ?.map((k, e) => MapEntry(k, e.toJson())));
  return val;
}

TriggerConfiguration _$TriggerConfigurationFromJson(
        Map<String, dynamic> json) =>
    TriggerConfiguration(
      sourceDeviceRoleName: json['sourceDeviceRoleName'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$TriggerConfigurationToJson(
    TriggerConfiguration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sourceDeviceRoleName', instance.sourceDeviceRoleName);
  return val;
}

ElapsedTimeTrigger _$ElapsedTimeTriggerFromJson(Map<String, dynamic> json) =>
    ElapsedTimeTrigger(
      sourceDeviceRoleName: json['sourceDeviceRoleName'] as String?,
      elapsedTime: _$IsoDurationFromJson(json['elapsedTime'] as String?),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$ElapsedTimeTriggerToJson(ElapsedTimeTrigger instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sourceDeviceRoleName', instance.sourceDeviceRoleName);
  writeNotNull('elapsedTime', _$IsoDurationToJson(instance.elapsedTime));
  return val;
}

ManualTrigger _$ManualTriggerFromJson(Map<String, dynamic> json) =>
    ManualTrigger(
      sourceDeviceRoleName: json['sourceDeviceRoleName'] as String?,
      label: json['label'] as String?,
      description: json['description'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$ManualTriggerToJson(ManualTrigger instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sourceDeviceRoleName', instance.sourceDeviceRoleName);
  writeNotNull('label', instance.label);
  writeNotNull('description', instance.description);
  return val;
}

ScheduledTrigger _$ScheduledTriggerFromJson(Map<String, dynamic> json) =>
    ScheduledTrigger(
      sourceDeviceRoleName: json['sourceDeviceRoleName'] as String?,
      time: TimeOfDay.fromJson(json['time'] as Map<String, dynamic>),
      recurrenceRule: RecurrenceRule.fromJson(
          json['recurrenceRule'] as Map<String, dynamic>),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$ScheduledTriggerToJson(ScheduledTrigger instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sourceDeviceRoleName', instance.sourceDeviceRoleName);
  val['time'] = instance.time.toJson();
  val['recurrenceRule'] = instance.recurrenceRule.toJson();
  return val;
}

TimeOfDay _$TimeOfDayFromJson(Map<String, dynamic> json) => TimeOfDay(
      hour: (json['hour'] as num?)?.toInt() ?? 0,
      minute: (json['minute'] as num?)?.toInt() ?? 0,
      second: (json['second'] as num?)?.toInt() ?? 0,
    );

Map<String, dynamic> _$TimeOfDayToJson(TimeOfDay instance) => <String, dynamic>{
      'hour': instance.hour,
      'minute': instance.minute,
      'second': instance.second,
    };

RecurrenceRule _$RecurrenceRuleFromJson(Map<String, dynamic> json) =>
    RecurrenceRule(
      $enumDecode(_$FrequencyEnumMap, json['frequency']),
      interval: (json['interval'] as num?)?.toInt() ?? 1,
      end: json['end'] == null
          ? null
          : End.fromJson(json['end'] as Map<String, dynamic>),
    );

Map<String, dynamic> _$RecurrenceRuleToJson(RecurrenceRule instance) =>
    <String, dynamic>{
      'frequency': _$FrequencyEnumMap[instance.frequency]!,
      'interval': instance.interval,
      'end': instance.end.toJson(),
    };

const _$FrequencyEnumMap = {
  Frequency.SECONDLY: 'SECONDLY',
  Frequency.MINUTELY: 'MINUTELY',
  Frequency.HOURLY: 'HOURLY',
  Frequency.DAILY: 'DAILY',
  Frequency.WEEKLY: 'WEEKLY',
  Frequency.MONTHLY: 'MONTHLY',
  Frequency.YEARLY: 'YEARLY',
};

End _$EndFromJson(Map<String, dynamic> json) => End(
      $enumDecode(_$EndTypeEnumMap, json['type']),
      elapsedTime: json['elapsedTime'] == null
          ? null
          : Duration(microseconds: (json['elapsedTime'] as num).toInt()),
      count: (json['count'] as num?)?.toInt(),
    );

Map<String, dynamic> _$EndToJson(End instance) {
  final val = <String, dynamic>{
    'type': _$EndTypeEnumMap[instance.type]!,
  };

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('elapsedTime', instance.elapsedTime?.inMicroseconds);
  writeNotNull('count', instance.count);
  return val;
}

const _$EndTypeEnumMap = {
  EndType.UNTIL: 'UNTIL',
  EndType.COUNT: 'COUNT',
  EndType.NEVER: 'NEVER',
};

SamplingConfiguration _$SamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    SamplingConfiguration()..$type = json['__type'] as String?;

Map<String, dynamic> _$SamplingConfigurationToJson(
    SamplingConfiguration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  return val;
}

NoOptionsSamplingConfiguration _$NoOptionsSamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    NoOptionsSamplingConfiguration()..$type = json['__type'] as String?;

Map<String, dynamic> _$NoOptionsSamplingConfigurationToJson(
    NoOptionsSamplingConfiguration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  return val;
}

BatteryAwareSamplingConfiguration _$BatteryAwareSamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    BatteryAwareSamplingConfiguration(
      normal: SamplingConfiguration.fromJson(
          json['normal'] as Map<String, dynamic>),
      low: SamplingConfiguration.fromJson(json['low'] as Map<String, dynamic>),
      critical: json['critical'] == null
          ? null
          : SamplingConfiguration.fromJson(
              json['critical'] as Map<String, dynamic>),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$BatteryAwareSamplingConfigurationToJson(
    BatteryAwareSamplingConfiguration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['normal'] = instance.normal.toJson();
  val['low'] = instance.low.toJson();
  writeNotNull('critical', instance.critical?.toJson());
  return val;
}

GranularitySamplingConfiguration _$GranularitySamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    GranularitySamplingConfiguration(
      $enumDecode(_$GranularityEnumMap, json['granularity']),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$GranularitySamplingConfigurationToJson(
        GranularitySamplingConfiguration instance) =>
    <String, dynamic>{
      '__type': instance.$type,
      'granularity': _$GranularityEnumMap[instance.granularity]!,
    };

const _$GranularityEnumMap = {
  Granularity.Detailed: 'Detailed',
  Granularity.Balanced: 'Balanced',
  Granularity.Coarse: 'Coarse',
};

DataType _$DataTypeFromJson(Map<String, dynamic> json) => DataType(
      json['namespace'] as String,
      json['name'] as String,
    );

Map<String, dynamic> _$DataTypeToJson(DataType instance) => <String, dynamic>{
      'namespace': instance.namespace,
      'name': instance.name,
    };

Data _$DataFromJson(Map<String, dynamic> json) =>
    Data()..$type = json['__type'] as String?;

Map<String, dynamic> _$DataToJson(Data instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  return val;
}

Acceleration _$AccelerationFromJson(Map<String, dynamic> json) => Acceleration(
      x: (json['x'] as num?)?.toDouble() ?? 0,
      y: (json['y'] as num?)?.toDouble() ?? 0,
      z: (json['z'] as num?)?.toDouble() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$AccelerationToJson(Acceleration instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['x'] = instance.x;
  val['y'] = instance.y;
  val['z'] = instance.z;
  return val;
}

Rotation _$RotationFromJson(Map<String, dynamic> json) => Rotation(
      x: (json['x'] as num?)?.toDouble() ?? 0,
      y: (json['y'] as num?)?.toDouble() ?? 0,
      z: (json['z'] as num?)?.toDouble() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$RotationToJson(Rotation instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['x'] = instance.x;
  val['y'] = instance.y;
  val['z'] = instance.z;
  return val;
}

MagneticField _$MagneticFieldFromJson(Map<String, dynamic> json) =>
    MagneticField(
      x: (json['x'] as num?)?.toDouble() ?? 0,
      y: (json['y'] as num?)?.toDouble() ?? 0,
      z: (json['z'] as num?)?.toDouble() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$MagneticFieldToJson(MagneticField instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['x'] = instance.x;
  val['y'] = instance.y;
  val['z'] = instance.z;
  return val;
}

Geolocation _$GeolocationFromJson(Map<String, dynamic> json) => Geolocation(
      latitude: (json['latitude'] as num?)?.toDouble() ?? 0,
      longitude: (json['longitude'] as num?)?.toDouble() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$GeolocationToJson(Geolocation instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['latitude'] = instance.latitude;
  val['longitude'] = instance.longitude;
  return val;
}

SignalStrength _$SignalStrengthFromJson(Map<String, dynamic> json) =>
    SignalStrength(
      rssi: (json['rssi'] as num?)?.toInt() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$SignalStrengthToJson(SignalStrength instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['rssi'] = instance.rssi;
  return val;
}

StepCount _$StepCountFromJson(Map<String, dynamic> json) => StepCount(
      steps: (json['steps'] as num?)?.toInt() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$StepCountToJson(StepCount instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['steps'] = instance.steps;
  return val;
}

HeartRate _$HeartRateFromJson(Map<String, dynamic> json) => HeartRate(
      bpm: (json['bpm'] as num?)?.toInt() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$HeartRateToJson(HeartRate instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['bpm'] = instance.bpm;
  return val;
}

ECG _$ECGFromJson(Map<String, dynamic> json) => ECG(
      milliVolt: (json['milliVolt'] as num?)?.toDouble() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$ECGToJson(ECG instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['milliVolt'] = instance.milliVolt;
  return val;
}

EDA _$EDAFromJson(Map<String, dynamic> json) => EDA(
      microSiemens: (json['microSiemens'] as num?)?.toDouble() ?? 0,
    )
      ..$type = json['__type'] as String?
      ..sensorSpecificData = json['sensorSpecificData'] == null
          ? null
          : Data.fromJson(json['sensorSpecificData'] as Map<String, dynamic>);

Map<String, dynamic> _$EDAToJson(EDA instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('sensorSpecificData', instance.sensorSpecificData?.toJson());
  val['microSiemens'] = instance.microSiemens;
  return val;
}

CompletedTask _$CompletedTaskFromJson(Map<String, dynamic> json) =>
    CompletedTask(
      taskName: json['taskName'] as String,
      taskData: json['taskData'] == null
          ? null
          : Data.fromJson(json['taskData'] as Map<String, dynamic>),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$CompletedTaskToJson(CompletedTask instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['taskName'] = instance.taskName;
  writeNotNull('taskData', instance.taskData?.toJson());
  return val;
}

TriggeredTask _$TriggeredTaskFromJson(Map<String, dynamic> json) =>
    TriggeredTask(
      triggerId: (json['triggerId'] as num).toInt(),
      taskName: json['taskName'] as String,
      destinationDeviceRoleName: json['destinationDeviceRoleName'] as String,
      control: $enumDecode(_$ControlEnumMap, json['control']),
      triggerData: json['triggerData'] == null
          ? null
          : Data.fromJson(json['triggerData'] as Map<String, dynamic>),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$TriggeredTaskToJson(TriggeredTask instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['triggerId'] = instance.triggerId;
  val['taskName'] = instance.taskName;
  val['destinationDeviceRoleName'] = instance.destinationDeviceRoleName;
  val['control'] = _$ControlEnumMap[instance.control]!;
  writeNotNull('triggerData', instance.triggerData?.toJson());
  return val;
}

Error _$ErrorFromJson(Map<String, dynamic> json) => Error(
      message: json['message'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$ErrorToJson(Error instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['message'] = instance.message;
  return val;
}

CustomInput _$CustomInputFromJson(Map<String, dynamic> json) => CustomInput(
      value: json['value'],
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$CustomInputToJson(CustomInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('value', instance.value);
  return val;
}

SexInput _$SexInputFromJson(Map<String, dynamic> json) => SexInput(
      value: $enumDecode(_$SexEnumMap, json['value']),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$SexInputToJson(SexInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['value'] = _$SexEnumMap[instance.value]!;
  return val;
}

const _$SexEnumMap = {
  Sex.Male: 'Male',
  Sex.Female: 'Female',
  Sex.Intersex: 'Intersex',
};

PhoneNumberInput _$PhoneNumberInputFromJson(Map<String, dynamic> json) =>
    PhoneNumberInput(
      countryCode: json['countryCode'] as String,
      number: json['number'] as String,
    )
      ..$type = json['__type'] as String?
      ..icoCode = json['icoCode'] as String?;

Map<String, dynamic> _$PhoneNumberInputToJson(PhoneNumberInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['countryCode'] = instance.countryCode;
  writeNotNull('icoCode', instance.icoCode);
  val['number'] = instance.number;
  return val;
}

SocialSecurityNumberInput _$SocialSecurityNumberInputFromJson(
        Map<String, dynamic> json) =>
    SocialSecurityNumberInput(
      socialSecurityNumber: json['socialSecurityNumber'] as String,
      country: json['country'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$SocialSecurityNumberInputToJson(
    SocialSecurityNumberInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['socialSecurityNumber'] = instance.socialSecurityNumber;
  val['country'] = instance.country;
  return val;
}

FullNameInput _$FullNameInputFromJson(Map<String, dynamic> json) =>
    FullNameInput(
      firstName: json['firstName'] as String?,
      middleName: json['middleName'] as String?,
      lastName: json['lastName'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$FullNameInputToJson(FullNameInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('firstName', instance.firstName);
  writeNotNull('middleName', instance.middleName);
  writeNotNull('lastName', instance.lastName);
  return val;
}

InformedConsentInput _$InformedConsentInputFromJson(
        Map<String, dynamic> json) =>
    InformedConsentInput(
      signedTimestamp: json['signedTimestamp'] == null
          ? null
          : DateTime.parse(json['signedTimestamp'] as String),
      signedLocation: json['signedLocation'] as String?,
      userId: json['userId'] as String,
      name: json['name'] as String,
      consent: json['consent'] as String,
      signatureImage: json['signatureImage'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$InformedConsentInputToJson(
    InformedConsentInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  val['signedTimestamp'] = instance.signedTimestamp.toIso8601String();
  writeNotNull('signedLocation', instance.signedLocation);
  val['userId'] = instance.userId;
  val['name'] = instance.name;
  val['consent'] = instance.consent;
  val['signatureImage'] = instance.signatureImage;
  return val;
}

AddressInput _$AddressInputFromJson(Map<String, dynamic> json) => AddressInput(
      address1: json['address1'] as String?,
      address2: json['address2'] as String?,
      street: json['street'] as String?,
      city: json['city'] as String?,
      postalCode: json['postalCode'] as String?,
      country: json['country'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$AddressInputToJson(AddressInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('address1', instance.address1);
  writeNotNull('address2', instance.address2);
  writeNotNull('street', instance.street);
  writeNotNull('city', instance.city);
  writeNotNull('postalCode', instance.postalCode);
  writeNotNull('country', instance.country);
  return val;
}

DiagnosisInput _$DiagnosisInputFromJson(Map<String, dynamic> json) =>
    DiagnosisInput(
      effectiveDate: json['effectiveDate'] == null
          ? null
          : DateTime.parse(json['effectiveDate'] as String),
      diagnosis: json['diagnosis'] as String?,
      icd11Code: json['icd11Code'] as String,
      conclusion: json['conclusion'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$DiagnosisInputToJson(DiagnosisInput instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('__type', instance.$type);
  writeNotNull('effectiveDate', instance.effectiveDate?.toIso8601String());
  writeNotNull('diagnosis', instance.diagnosis);
  val['icd11Code'] = instance.icd11Code;
  writeNotNull('conclusion', instance.conclusion);
  return val;
}
